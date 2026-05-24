package dev.novov.duckdb.runner.output;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.runner.BenchRunner;
import dev.novov.duckdb.runner.CaseDescriptions;
import dev.novov.duckdb.runner.ResultTablePrinter;
import dev.novov.duckdb.runner.RunnerOptions;

import java.util.List;
import java.util.stream.Collectors;

public final class PlainRunnerOutput implements RunnerOutput {
    @Override
    public void printRunHeader(RunnerOptions options, List<QueryCase> cases, List<AnalyticsEngine> engines) {
        System.out.println("Dataset=" + options.datasetOption());
        System.out.println("Engines: " + engines.stream().map(AnalyticsEngine::name).collect(Collectors.joining(", ")));
        System.out.printf("Threads=%d warmup=%d runs=%d limitRows=%d explain=%b%n",
                options.threads(), options.warmups(), options.runs(), options.limitRows(), options.explain());
        cases.forEach(queryCase -> System.out.println("Case " + queryCase.id() + " -> " + CaseDescriptions.describe(queryCase)));
    }

    @Override
    public void printCaseStarted(QueryCase queryCase, AnalyticsEngine engine) {
        System.out.printf("Running %s on %s...%n", queryCase.id(), engine.name());
    }

    @Override
    public void printCaseFinished(QueryCase queryCase, String engineName, CaseResult result) {
    }

    @Override
    public void printRunFinished(List<BenchRunner.BenchResult> results, int warmups, int runs) {
        ResultTablePrinter.print(results, warmups, runs);
    }

    @Override
    public void printInfo(String message) {
        System.out.println(message);
    }

    @Override
    public void printError(String message) {
        System.err.println(message);
    }
}
