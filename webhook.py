import json
import os
import aiosqlite
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from arq import create_pool
from arq.connections import RedisSettings

DB_PATH = os.environ.get("DB_PATH", "/data/archive.db")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "change_me")
REDIS = RedisSettings.from_dsn(os.environ["REDIS_URL"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await create_pool(REDIS)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS inbox (
                update_id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL,
                processed INTEGER DEFAULT 0,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    yield
    await app.state.redis.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/webhook/{secret}")
async def webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        return Response(status_code=403)

    update = await request.json()
    update_id = update.get("update_id")
    if update_id is None:
        return Response(status_code=400)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO inbox (update_id, payload) VALUES (?, ?)",
                (update_id, json.dumps(update, ensure_ascii=False)),
            )
            await db.commit()
    except Exception:
        return Response(status_code=503)

    await request.app.state.redis.enqueue_job("process_update", update_id)
    return Response(status_code=200)