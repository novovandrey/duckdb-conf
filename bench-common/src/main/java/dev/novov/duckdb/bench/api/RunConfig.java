package dev.novov.duckdb.bench.api;

public record RunConfig(
        int warmupRuns,
        int measuredRuns,
        int threads,
        long limitRowsOrMinusOne,
        boolean explain,
        boolean coldJVM
) {
    public RunConfig {
        if (warmupRuns < 0 || measuredRuns < 1) {
            throw new IllegalArgumentException("Invalid run counts: warmups must be >= 0 and measured >= 1");
        }
    }
}
