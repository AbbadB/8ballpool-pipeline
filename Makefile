.PHONY: test up down demo logs
test:        ; python -m pytest -q
up:          ; docker compose up -d --build
down:        ; docker compose down -v
demo:        ; docker compose up --build
logs:        ; docker compose logs -f
