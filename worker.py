import json
import os
import aiosqlite
from arq import cron
from arq.connections import RedisSettings
from aiogram import Bot

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = 7989081494
DB_PATH = os.environ.get("DB_PATH", "/data/archive.db")
MEDIA_DIR = os.environ.get("MEDIA_DIR", "/data/media")
os.makedirs(MEDIA_DIR, exist_ok=True)

bot = Bot(BOT_TOKEN)

async def _claim(update_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "UPDATE inbox SET processed = 1 WHERE update_id = ? AND processed = 0",
            (update_id,),
        )
        if cur.rowcount != 1:
            return None
        await db.commit()
        row = await db.execute_fetchone(
            "SELECT payload FROM inbox WHERE update_id = ?", (update_id,)
        )
        if not row:
            return None
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            await db.execute(
                "UPDATE inbox SET processed = 2 WHERE update_id = ?", (update_id,)
            )
            await db.commit()
            return None

async def process_update(ctx, update_id: int):
    payload = await _claim(update_id)
    if payload is None:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # создаём таблицы если нет
        await db.execute("""
            CREATE TABLE IF NOT EXISTS archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                sender_id INTEGER,
                sender_name TEXT,
                text TEXT,
                media_path TEXT,
                connection_id TEXT,
                deleted_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, message_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS deletions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id TEXT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                deleted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, message_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                old_text TEXT,
                new_text TEXT,
                edit_date TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

        # --- НОВОЕ СООБЩЕНИЕ ---
        msg = payload.get("business_message")
        if msg:
            chat_id = msg["chat"]["id"]
            msg_id = msg["message_id"]
            text = msg.get("text") or msg.get("caption")
            sender = msg.get("from", {})
            sender_id = sender.get("id")
            sender_name = sender.get("first_name", "") + " " + sender.get("last_name", "")
            sender_name = sender_name.strip() or "unknown"

            media_path = None
            if "photo" in msg:
                file_id = msg["photo"][-1]["file_id"]
                file = await bot.get_file(file_id)
                media_path = f"{MEDIA_DIR}/{chat_id}_{msg_id}.jpg"
                await bot.download_file(file.file_path, media_path)
            elif "video" in msg:
                file_id = msg["video"]["file_id"]
                file = await bot.get_file(file_id)
                media_path = f"{MEDIA_DIR}/{chat_id}_{msg_id}.mp4"
                await bot.download_file(file.file_path, media_path)

            await db.execute(
                """INSERT OR REPLACE INTO archive
                   (chat_id, message_id, sender_id, sender_name, text, media_path, connection_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (chat_id, msg_id, sender_id, sender_name, text, media_path,
                 payload.get("business_connection_id")),
            )
            await db.commit()

        # --- УДАЛЕНИЯ ---
        evt = payload.get("deleted_business_messages")
        if evt:
            chat_id = evt.get("chat", {}).get("id")
            ids = evt.get("message_ids", [])
            if chat_id and ids:
                for mid in ids:
                    await db.execute(
                        "INSERT OR IGNORE INTO deletions (connection_id, chat_id, message_id) VALUES (?, ?, ?)",
                        (evt.get("business_connection_id"), chat_id, mid),
                    )
                    await db.execute(
                        "UPDATE archive SET deleted_at = CURRENT_TIMESTAMP WHERE chat_id = ? AND message_id = ?",
                        (chat_id, mid),
                    )

                    row = await db.execute_fetchone(
                        """SELECT sender_name, text, media_path, created_at
                           FROM archive WHERE chat_id = ? AND message_id = ?""",
                        (chat_id, mid),
                    )
                    if row:
                        sender, text, media_path, created_at = row
                        when = created_at or "unknown time"
                        body = text or "[медиа/файл]"
                        note = f"🗑 УДАЛЕНО\nКто: {sender or 'unknown'}\nКогда: {when}\nЧто: {body}"
                        if media_path:
                            note += f"\nФайл: {media_path}"
                        try:
                            await bot.send_message(OWNER_ID, note)
                        except Exception as e:
                            print(f"[!] owner notify failed: {e}")

                await db.commit()

        # --- ПРАВКИ ---
        evt = payload.get("edited_business_message")
        if evt:
            chat_id = evt.get("chat", {}).get("id")
            msg_id = evt.get("message_id")
            new_text = evt.get("text") or evt.get("caption")
            edit_date = str(evt.get("edit_date"))
            sender = evt.get("from", {})
            sender_name = (sender.get("first_name", "") + " " + sender.get("last_name", "")).strip() or "unknown"

            if chat_id and msg_id:
                row = await db.execute_fetchone(
                    "SELECT text FROM archive WHERE chat_id = ? AND message_id = ?",
                    (chat_id, msg_id),
                )
                old_text = row[0] if row else None

                await db.execute(
                    "INSERT INTO revisions (chat_id, message_id, old_text, new_text, edit_date) VALUES (?, ?, ?, ?, ?)",
                    (chat_id, msg_id, old_text, new_text, edit_date),
                )
                await db.execute(
                    "UPDATE archive SET text = ? WHERE chat_id = ? AND message_id = ?",
                    (new_text, chat_id, msg_id),
                )
                await db.commit()

                note = (
                    f"✏️ ИЗМЕНЕНО\nКто: {sender_name}\nКогда: {edit_date}\n"
                    f"Было: {old_text or '[не сохранено]'}\nСтало: {new_text or '[медиа]'}"
                )
                try:
                    await bot.send_message(OWNER_ID, note)
                except Exception as e:
                    print(f"[!] owner notify failed: {e}")

async def check_pending(ctx):
    import aiohttp
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo") as r:
            data = await r.json()
    pending = data.get("result", {}).get("pending_update_count", 0)
    if pending > 50:
        print(f"[ALERT] pending={pending}")

class WorkerSettings:
    functions = [process_update]
    cron_jobs = [cron(check_pending, minute={0, 15, 30, 45})]
    redis_settings = RedisSettings.from_dsn(os.environ["REDIS_URL"])