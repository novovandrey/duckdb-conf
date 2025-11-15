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
