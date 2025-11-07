package dev.novov.duckdb.bench.api;

public sealed interface QueryCase permits GroupByCase, FilterCase, TopKCase {
    String id();
}
