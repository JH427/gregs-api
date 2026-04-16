.PHONY: start init-db smoke smoke-imports smoke-knowledge smoke-query

start:
	docker compose up -d --build

init-db:
	docker compose run --rm api python -m app.db_init

smoke:
	bash scripts/smoke.sh

smoke-imports:
	bash scripts/smoke_imports.sh

smoke-knowledge:
	bash scripts/smoke_knowledge.sh

smoke-query:
	bash scripts/smoke_query.sh
