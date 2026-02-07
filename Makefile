.PHONY: start init-db smoke

start:
	docker compose up -d --build

init-db:
	docker compose run --rm api python -m app.db_init

smoke:
	bash scripts/smoke.sh
