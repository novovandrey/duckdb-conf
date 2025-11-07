package dev.novov.duckdb.bench.api;

public interface AnalyticsEngine {
    CaseResult run(QueryCase queryCase, RunConfig config) throws Exception;

    String name();
}
