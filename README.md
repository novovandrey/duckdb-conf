# duckdb-conf
prerequisite:
java 21
maven
dataset: http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv
To run benchmarks below you need convert csv dataset to parquet woth coamd below:
duckdb -c "COPY (
  SELECT *
  FROM read_csv_auto('C:\Users\Andrei\IdeaProjects\data\pp-complete.csv')
) TO 'C:\Users\Andrei\IdeaProjects\data\pp-complete-test.parquet' (FORMAT PARQUET);"
Examples
duckdb -c "select count(*) from read_csv('C:\Users\Andrei\IdeaProjects\data\neighbourhoods.csv')"
duckdb -c "select count(*) from read_csv('https://data.insideairbnb.com/portugal/lisbon/lisbon/2025-09-21/data/listings.csv.gz')"
duckdb -c "select count(*) from read_parquet('https://zenodo.org/records/14537442/files/chicago-taxi.parquet?download=1')"

duckdb -c "COPY (
  SELECT *
  FROM 'C:\Users\Andrei\IdeaProjects\data\pp-complete.parquet'
  USING SAMPLE 1000000 ROWS
) TO pp-complete-test-1M.parquet' (FORMAT PARQUET);"

Test benchmarks
java21 -jar app-runner\target\app-runner-1.0-SNAPSHOT.jar --engine duckdb  --file "C:\Users\Andrei\IdeaProjects\data\pp-complete.parquet" --dataset ppd --case ppd_sales_by_year       --threads 6 --warmup 1 --runs 5
java21 -jar app-runner\target\app-runner-1.0-SNAPSHOT.jar --engine duckdb  --file "C:\Users\Andrei\IdeaProjects\data\pp-complete.parquet" --dataset ppd --case ppd_sales_by_year       --threads 1 --warmup 1 --runs 5
java21 '-Dhadoop.home.dir=C:\hadoop' -jar app-runner\target\app-runner-1.0-SNAPSHOT.jar --engine parquet --file "C:\Users\Andrei\IdeaProjects\data\pp-complete.parquet" --dataset ppd --case ppd_sales_by_year       --threads 6 --warmup 1 --runs 5
java21  '-Dhadoop.home.dir=C:\hadoop' -jar app-runner\target\app-runner-1.0-SNAPSHOT.jar --engine parquet --file "C:\Users\Andrei\IdeaProjects\data\pp-complete.parquet" --dataset ppd --case ppd_sales_by_year       --threads 1 --warmup 1 --runs 5
