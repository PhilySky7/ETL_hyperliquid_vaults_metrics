-include .env

export

makemigrations:
	uv run python3 database.py

run:
	uv run python3 main.py

run_hl_etl_in_container:
	docker compose build && \
	docker compose up -d db && \
	docker compose run --rm app uv run python3 database.py && \
	docker compose up -d

logs:
	docker compose logs -f

db_container_connect:
	docker compose exec -it db psql -U ${DB_USER} -d ${DB_NAME}