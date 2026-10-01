.PHONY: up down logs test lint demo scale reset

up:        ## build and start everything
	docker compose up -d --build
down:
	docker compose down
logs:      ## follow consumer logs
	docker compose logs -f consumer
test:
	pytest -v
lint:
	ruff check .
demo:
	./scripts/demo.sh
scale:     ## run 2 consumers -> partitions are split between them
	docker compose up -d --scale consumer=2
reset:     ## wipe Kafka data and the SQLite DB
	docker compose down -v
