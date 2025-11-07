package dev.novov.duckdb.bench.api;

public record HeadCase(String id, String file, int limit) implements QueryCase {
}