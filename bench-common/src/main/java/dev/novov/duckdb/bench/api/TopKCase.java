package dev.novov.duckdb.bench.api;

public record TopKCase(
        String id,
        String file,
        String orderByCol,
        int k,
        boolean desc
) implements QueryCase {
}
