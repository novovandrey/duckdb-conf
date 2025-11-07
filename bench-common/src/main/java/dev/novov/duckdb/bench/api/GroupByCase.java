package dev.novov.duckdb.bench.api;

public record GroupByCase(
        String id,
        String file,
        String groupCol,
        String aggCol,
        AggFn aggFn
) implements QueryCase {
}
