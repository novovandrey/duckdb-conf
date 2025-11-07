package dev.novov.duckdb.bench.api;

public record FilterCase(
        String id,
        String file,
        String filterExpr
) implements QueryCase {
}
