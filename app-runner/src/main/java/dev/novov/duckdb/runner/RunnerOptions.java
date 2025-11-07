package dev.novov.duckdb.runner;

record RunnerOptions(
        EngineOption engineOption,
        String caseFilter,
        String file,
        int threads,
        int warmups,
        int runs,
        boolean explain,
        long limitRows
) {
}
