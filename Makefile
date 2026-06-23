.PHONY: test lint up down demo logs replay-dlq
test:        ; python -m pytest -q
lint:        ; ruff check src apps tests
up:          ; docker compose up -d --build
down:        ; docker compose down -v
demo:        ; docker compose up --build
logs:        ; docker compose logs -f
replay-dlq:  ; KAFKA_BOOTSTRAP=localhost:29092 python apps/replay_dlq.py
