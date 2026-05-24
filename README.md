# duckdb-conf
prerequisite:\
java 21+
maven 3.8+

PPD_CSV_PATH for example "/data/datasets/pp-complete.csv"
PPD_DATASET_PATH for example "/data/datasets/pp-complete.parquet"
APP_DUCKDB_RUNNER for example "/apps/duckdb-benchmark/app-runner/target/app-runner-1.0-SNAPSHOT.jar"

**dataset**: http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv \
To run benchmarks below you need convert csv dataset to parquet with command below: \
duckdb -c "COPY (
  SELECT *
  FROM read_csv_auto(PPD_CSV_PATH)
) TO PPD_DATASET_PATH (FORMAT PARQUET);"

Examples \
duckdb -c "select count(*) from read_csv(PATH_TO_CSV)" \
duckdb -c "select count(*) from read_csv('https://data.insideairbnb.com/portugal/lisbon/lisbon/2025-09-21/data/listings.csv.gz')" \
duckdb -c "select count(*) from read_parquet('https://zenodo.org/records/14537442/files/chicago-taxi.parquet?download=1')" \

duckdb -c "COPY (
  SELECT *
  FROM PPD_DATASET_PATH
  USING SAMPLE 1000000 ROWS
) TO path\pp-complete-test-1M.parquet' (FORMAT PARQUET);"

Test benchmarks \
java21 -jar $APP_DUCKDB_RUNNER --engine duckdb  --file $PPD_DATASET_PATH --dataset ppd --case ppd_sales_by_year       --threads 6 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine duckdb  --file $PPD_DATASET_PATH --dataset ppd --case ppd_sales_by_year       --threads 1 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine parquet --file $PPD_DATASET_PATH --dataset ppd --case ppd_sales_by_year       --threads 6 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine parquet --file $PPD_DATASET_PATH --dataset ppd --case ppd_sales_by_year       --threads 1 --warmup 1 --runs 5

java21 -jar $APP_DUCKDB_RUNNER --engine duckdb  --file $PPD_DATASET_PATH --dataset ppd --case ppd_median_by_district --threads 6 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine duckdb  --file $PPD_DATASET_PATH --dataset ppd --case ppd_median_by_district --threads 1 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine parquet --file $PPD_DATASET_PATH --dataset ppd --case ppd_median_by_district --threads 6 --warmup 1 --runs 5
java21 -jar $APP_DUCKDB_RUNNER --engine parquet --file $PPD_DATASET_PATH --dataset ppd --case ppd_median_by_district --threads 1 --warmup 1 --runs 5

## New: conference demo mode

Runner now supports `--demo` for short, presentation-friendly terminal output.

Example:
`java -jar app-runner/target/app-runner-1.0-SNAPSHOT.jar --demo --engine duckdb --dataset ppd --case ppd_sales_by_year --file C:/data/pp-complete.parquet --threads 8 --warmup 0 --runs 1`

`--demo` keeps benchmark execution logic unchanged, only output formatting changes.

## New demo case

Added `cross_source_join` case (DuckDB only):
- fact source: PPD Parquet
- dimension source: CSV metadata
- query: JOIN + aggregation

This case uses env var `DUCKDB_DEMO_DISTRICT_CSV` for dimension CSV path.

## Stage-friendly scripts

Added shortcuts:
- `scripts/demo` (Unix-like)
- `scripts/demo.bat` (Windows)

Aliases:
- `01-parquet-scan`
- `02-duckdb-vs-parquet-java`
- `03-cross-source-join`
- `04-threads`
- `05-copy-to-parquet`

Run examples:
- `scripts/demo 01-parquet-scan`
- `scripts/demo 02-duckdb-vs-parquet-java`
- `scripts/demo 03-cross-source-join`
- `scripts/demo 04-threads`
- `scripts/demo 05-copy-to-parquet`

Windows:
- `scripts\\demo.bat 01-parquet-scan`

## Demo environment variables

Required:
- `DUCKDB_DEMO_DISTRICT_CSV`

Parquet input selection:
- `DUCKDB_DEMO_PPD_PARQUET` (default fallback)
- `DUCKDB_DEMO_PPD_PARQUET_FULL` (preferred for full dataset)
- `DUCKDB_DEMO_PPD_PARQUET_SMALL` (used in fast mode)

Fast mode:
- `DUCKDB_DEMO_FAST=true` -> script uses `DUCKDB_DEMO_PPD_PARQUET_SMALL`

CSV to Parquet demo:
- `DUCKDB_DEMO_PPD_CSV`

Scripts fail fast with clear messages when required env vars are missing.

## How to run demo mode (step-by-step)

### 1) Build project

From repository root:

`mvn clean package`

### 2) Prepare environment variables

#### Windows (PowerShell)

```powershell
$env:DUCKDB_DEMO_PPD_PARQUET_FULL = "C:/data/pp-complete.parquet"
$env:DUCKDB_DEMO_PPD_PARQUET_SMALL = "C:/data/pp-complete-1M.parquet"
$env:DUCKDB_DEMO_PPD_CSV = "C:/data/pp-complete.csv"
$env:DUCKDB_DEMO_DISTRICT_CSV = "C:/data/district_metadata.csv"
# optional fast mode:
$env:DUCKDB_DEMO_FAST = "true"
```

#### Linux/macOS (bash/zsh)

```bash
export DUCKDB_DEMO_PPD_PARQUET_FULL="/data/pp-complete.parquet"
export DUCKDB_DEMO_PPD_PARQUET_SMALL="/data/pp-complete-1M.parquet"
export DUCKDB_DEMO_PPD_CSV="/data/pp-complete.csv"
export DUCKDB_DEMO_DISTRICT_CSV="/data/district_metadata.csv"
# optional fast mode:
export DUCKDB_DEMO_FAST=true
```

### 3) Run one of stage aliases

#### Unix-like

```bash
scripts/demo 01-parquet-scan
scripts/demo 02-duckdb-vs-parquet-java
scripts/demo 03-cross-source-join
scripts/demo 04-threads
scripts/demo 05-copy-to-parquet
```

#### Windows

```bat
scripts\demo.bat 01-parquet-scan
scripts\demo.bat 02-duckdb-vs-parquet-java
scripts\demo.bat 03-cross-source-join
scripts\demo.bat 04-threads
scripts\demo.bat 05-copy-to-parquet
```

### 4) Run directly via jar (without scripts)

```bash
java -jar app-runner/target/app-runner-1.0-SNAPSHOT.jar --demo --engine duckdb --dataset ppd --case ppd_sales_by_year --file C:/data/pp-complete.parquet --threads 8 --warmup 0 --runs 1
```

### 5) Quick verification

Check options:

`java -jar app-runner/target/app-runner-1.0-SNAPSHOT.jar --help`

You should see `--demo` in help output.
