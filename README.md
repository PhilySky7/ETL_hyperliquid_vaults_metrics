# Hyperliquid ETL

ETL for collecting vault data, calculating 30 metrics and writing to PostgreSQL.

## Requirements

- Python 3.12
- PostgreSQL 13+
- [uv](https://docs.astral.sh/uv/getting-started/) (for dependency management)

## Installation

Install uv:

```bash
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
# On Windows.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Or, from [PyPI](https://pypi.org/project/uv/):

```bash
# With pip.
pip install uv
```

## Documentation

uv's documentation is available at [docs.astral.sh/uv](https://docs.astral.sh/uv).

## Project

### Preparation
**To run this project do the following steps:**

1. Set virtual environment:
```bash
uv venv
```

2. Install venv:
```bash
source .venv/bin/activate
```

3. Install dependencies:
```bash
uv sync
```

4. Copy environment sample
```bash
cp .env_sample .env
```

5. Run project:

Locally:
```bash
make run
```

In container:
```bash
make run_hl_etl_in_container
```

### Database Migration
Create a schema before the first run through the Makefile:
```bash
make makemigrations
```

### ETL Launch
Sequential run (vaultsAddresses → vaultDetails → userFills → metrics → upsert):
```bash
uv run python3 main.py
```

Or, use Makefile command:

```bash
make run
```

The logs of the INFO level display the key stages of execution.

> For better perfomance set optimal BATCH_SIZE in `.env`.

### Access to PostgreSQL
```bash
docker compose exec -it db psql -U postgres -d hyperliquid
```
- db: service name in docker-compose.yml
- postgres: DB_USER from `.env`
- hyperliquid: DB_NAME from `.env`

Or, use Makefile command:
```bash
make db_container_connect
```

### Project Structure
```
project/
├── api_client.py      # HTTP client: get_vault_addresses, fetch_details (async)
├── database.py        # Database connection, run_migration, upsert_vault_data
├── main.py            # Main script
├── metrics.py         # Metrics calculation (performance, risk, trading, trend, capital, efficiency)
├── schema.sql         # SQL schema of the vaults table
├── pyproject.toml     # Dependencies
├── uv.lock            # lock dependencie's versions
├── Dockerfile         # Building app environment
├── docker-compose.yml # Containers manager
├── Makefile           # Fast usage
└── .env_sample        # .env sample
```

Key functions:
- `database.get_connection()` — connection with PostgreSQL.
- `database.run_migration(path)` — apply `schema.sql`.
- `database.upsert_vault_data(conn, data)` — idempotent upsert (`ON CONFLICT`).
- `api_client.get_vault_addresses()` — list of vaultAdress.
- `api_client.fetch_details(body_field, addresses)` — load `vaultDetails` or `userFills`.
- `main.build_vault(address, detail, fills)` — collects all 30 metrics into one dictionary for upsert.

### Logging
Default setup, `main.py`:
```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
```

### Makefile
```bash
make makemigrations          # apply schema.sql via database.py
make run                     # run python3 main.py
make run_hl_etl_in_container # run project in container
make logs                    # view logs
make db_container_connect    # connect to db in container
```

### Possible issues
- Unable to connect to the database: check the `.env`, database availability, and user rights.
- Slow processing: try with a small BATCH_SIZE at first.
- Empty data/metric errors: functions in `metrics.py ` are protected from `None`, but check if the API formats match.
- Pay attention to Average Recovery Days, Average Position Holding Time calculating.
- In addition, there is a fallback both for TVL and Vault Age days.

## License
MIT
