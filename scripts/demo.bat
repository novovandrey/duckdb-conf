@echo off
setlocal enabledelayedexpansion

set "ROOT_DIR=%~dp0.."
set "JAR_PATH=%ROOT_DIR%\app-runner\target\app-runner-1.0-SNAPSHOT.jar"
set "ALIAS=%~1"

if "%ALIAS%"=="" (
  echo Usage: scripts\demo.bat ^<alias^>
  exit /b 1
)

set "PPD_PARQUET="
if /I "%DUCKDB_DEMO_FAST%"=="true" (
  if "%DUCKDB_DEMO_PPD_PARQUET_SMALL%"=="" (
    echo Missing required environment variable: DUCKDB_DEMO_PPD_PARQUET_SMALL
    exit /b 2
  )
  set "PPD_PARQUET=%DUCKDB_DEMO_PPD_PARQUET_SMALL%"
) else (
  if not "%DUCKDB_DEMO_PPD_PARQUET_FULL%"=="" (
    set "PPD_PARQUET=%DUCKDB_DEMO_PPD_PARQUET_FULL%"
  ) else (
    if "%DUCKDB_DEMO_PPD_PARQUET%"=="" (
      echo Missing required environment variable: DUCKDB_DEMO_PPD_PARQUET
      exit /b 2
    )
    set "PPD_PARQUET=%DUCKDB_DEMO_PPD_PARQUET%"
  )
)

if "%DUCKDB_DEMO_DISTRICT_CSV%"=="" (
  echo Missing required environment variable: DUCKDB_DEMO_DISTRICT_CSV
  exit /b 2
)

if "%ALIAS%"=="01-parquet-scan" goto demo1
if "%ALIAS%"=="02-duckdb-vs-parquet-java" goto demo2
if "%ALIAS%"=="03-cross-source-join" goto demo3
if "%ALIAS%"=="04-threads" goto demo4
if "%ALIAS%"=="05-copy-to-parquet" goto demo5

echo Unknown alias: %ALIAS%
exit /b 1

:demo1
java -jar "%JAR_PATH%" --demo --engine duckdb --dataset ppd --case ppd_median_by_district --file "%PPD_PARQUET%" --threads 8 --warmup 0 --runs 1
exit /b %errorlevel%

:demo2
java -jar "%JAR_PATH%" --demo --engine both --dataset ppd --case ppd_new_vs_old --file "%PPD_PARQUET%" --threads 8 --warmup 0 --runs 1
exit /b %errorlevel%

:demo3
java -jar "%JAR_PATH%" --demo --engine duckdb --dataset ppd --case cross_source_join --file "%PPD_PARQUET%" --threads 8 --warmup 0 --runs 1
exit /b %errorlevel%

:demo4
java -jar "%JAR_PATH%" --demo --engine duckdb --dataset ppd --case ppd_sales_by_year --file "%PPD_PARQUET%" --threads 1 --warmup 0 --runs 1
if errorlevel 1 exit /b %errorlevel%
java -jar "%JAR_PATH%" --demo --engine duckdb --dataset ppd --case ppd_sales_by_year --file "%PPD_PARQUET%" --threads 8 --warmup 0 --runs 1
exit /b %errorlevel%

:demo5
if "%DUCKDB_DEMO_PPD_CSV%"=="" (
  echo Missing required environment variable: DUCKDB_DEMO_PPD_CSV
  exit /b 2
)
if "%DUCKDB_DEMO_PPD_PARQUET_SMALL%"=="" (
  set "OUT=%ROOT_DIR%\tmp\demo-output.parquet"
) else (
  set "OUT=%DUCKDB_DEMO_PPD_PARQUET_SMALL%"
)
java -jar "%JAR_PATH%" --engine duckdb --dataset ppd --file "%DUCKDB_DEMO_PPD_CSV%" --to-parquet "%OUT%" --threads 8
if errorlevel 1 exit /b %errorlevel%
java -jar "%JAR_PATH%" --demo --engine duckdb --dataset ppd --case ppd_avg_by_district --file "%OUT%" --threads 8 --warmup 0 --runs 1
exit /b %errorlevel%
