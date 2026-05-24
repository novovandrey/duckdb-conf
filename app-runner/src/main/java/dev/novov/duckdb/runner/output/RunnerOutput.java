package dev.novov.duckdb.runner.output;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.runner.BenchRunner;
import dev.novov.duckdb.runner.RunnerOptions;

import java.util.List;

public interface RunnerOutput {
    void printRunHeader(RunnerOptions options, List<QueryCase> cases, List<AnalyticsEngine> engines);

    void printCaseStarted(QueryCase queryCase, AnalyticsEngine engine);

    void printCaseFinished(QueryCase queryCase, String engineName, CaseResult result);

    void printRunFinished(List<BenchRunner.BenchResult> results, int warmups, int runs);

    void printInfo(String message);

    void printError(String message);
}
