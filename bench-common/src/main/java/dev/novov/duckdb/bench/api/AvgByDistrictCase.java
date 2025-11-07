package dev.novov.duckdb.bench.api;

public record AvgByDistrictCase(String id, String file, int minCount) implements QueryCase {
}
