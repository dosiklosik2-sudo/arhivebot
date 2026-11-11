#!/bin/sh
set -e

# запускаем воркер в фоне
arq worker.WorkerSettings &

# запускаем вебхук на переднем плане (Railway даёт $PORT)
exec uvicorn webhook:app --host 0.0.0.0 --port $PORT