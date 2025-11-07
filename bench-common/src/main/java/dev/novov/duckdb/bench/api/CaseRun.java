package dev.novov.duckdb.bench.api;

public record CaseRun(
        long nanos,
        long rowsOut,
        long bytesRead,
        long maxUsedMemBytes
) {
}
