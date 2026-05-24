package dev.novov.duckdb.bench.api;

public record CrossSourceJoinCase(
        String id,
        String factParquet,
        String districtCsv
) implements QueryCase {
}
