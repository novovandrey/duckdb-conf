package dev.novov.duckdb.bench.api;

public record MedianByDistrictCase(String id, String file, int minCount, int limit) implements QueryCase {
}
