package dev.novov.duckdb.runner.demo;

import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.runner.CaseDescriptions;

import java.util.Map;

public final class DemoCatalog {
    private static final Map<String, DemoMeta> KNOWN = Map.of(
            "ppd_sales_by_year", new DemoMeta("04", "Threads 1 vs 8", "Show automatic parallelism on analytical queries",
                    "DuckDB benefits from multicore execution without application-level parallel code."),
            "ppd_new_vs_old", new DemoMeta("02", "DuckDB vs parquet-java", "Compare SQL engine with manual Java aggregation",
                    "DuckDB gives SQL-level expressiveness with competitive local performance."),
            "ppd_median_by_district", new DemoMeta("01", "Direct Parquet Scan", "Query Parquet directly from Java",
                    "DuckDB reads Parquet directly. No import step."),
            "cross_source_join", new DemoMeta("03", "Cross-source JOIN", "Join Parquet fact data with CSV metadata",
                    "DuckDB is not only a fast Parquet reader. It is a local SQL engine over multiple data sources."),
            "ppd_avg_by_district", new DemoMeta("05", "Copy to Parquet", "Run analytical query after CSV to Parquet flow",
                    "Parquet conversion is one command in the same runner.")
    );

    private DemoCatalog() {
    }

    public static DemoMeta resolve(QueryCase queryCase) {
        DemoMeta known = KNOWN.get(queryCase.id());
        if (known != null) {
            return known;
        }
        return new DemoMeta("00", queryCase.id(), CaseDescriptions.describe(queryCase), "DuckDB local analytics benchmark completed.");
    }

    public record DemoMeta(String number, String title, String goal, String takeaway) {
    }
}
