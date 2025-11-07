package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class BenchRunner {
    private final EngineFactory engineFactory = new EngineFactory();
    private final CaseSelector caseSelector = new CaseSelector();

    void run(RunnerOptions options) throws Exception {
        List<QueryCase> cases = caseSelector.select(options.caseFilter(), options.file());
        if (cases.isEmpty()) {
            throw new IllegalStateException("No cases selected");
        }
        List<AnalyticsEngine> engines = engineFactory.create(options.engineOption());
        System.out.println("Engines: " + engines.stream().map(AnalyticsEngine::name).collect(Collectors.joining(", ")));
        System.out.printf("Threads=%d warmup=%d runs=%d limitRows=%d explain=%b%n",
                options.threads(), options.warmups(), options.runs(), options.limitRows(), options.explain());
        cases.forEach(queryCase -> System.out.println("Case " + queryCase.id() + " -> " + CaseDescriptions.describe(queryCase)));

        RunConfig config = new RunConfig(
                options.warmups(),
                options.runs(),
                options.threads(),
                options.limitRows(),
                options.explain(),
                false
        );

        List<BenchResult> results = new ArrayList<>();
        for (QueryCase queryCase : cases) {
            for (AnalyticsEngine engine : engines) {
                System.out.printf("Running %s on %s...%n", queryCase.id(), engine.name());
                CaseResult result = engine.run(queryCase, config);
                results.add(new BenchResult(queryCase, engine.name(), result));
            }
        }

        ResultTablePrinter.print(results, options.warmups(), options.runs());
    }

    record BenchResult(QueryCase queryCase, String engine, CaseResult result) {
    }
}
