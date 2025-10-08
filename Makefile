makemigrations:
	python3 database.py

run:
	python3 main.py

run_hl_etl_in_container:
	docker compose build && \
	docker compose up -d db && \
	docker compose run --rm app python3 database.py && \
	docker compose up -d

logs:
	docker compose logs -f