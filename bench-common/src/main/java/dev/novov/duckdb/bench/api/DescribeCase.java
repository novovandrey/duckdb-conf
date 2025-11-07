package dev.novov.duckdb.bench.api;

public record DescribeCase(String id, String file) implements QueryCase {
}