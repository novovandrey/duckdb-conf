package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.DescribeCase;
import dev.novov.duckdb.bench.api.HeadCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.Stopwatch;
import dev.novov.duckdb.engines.DuckdbEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

final class BenchRunner {
    private final EngineFactory engineFactory = new EngineFactory();
    private final CaseSelector caseSelector = new CaseSelector();

    void run(RunnerOptions options) throws Exception {
        if (options.toParquet() != null) {
            runCsvToParquet(options);
            return;
        }
        if (options.schemaOnly()) {
            ensureDuckdbEngine(options.engineOption());
            runInteractive(options, new DescribeCase("ppd_describe", options.file()));
            return;
        }
        if (options.headLimit() > 0) {
            ensureDuckdbEngine(options.engineOption());
            runInteractive(options, new HeadCase("ppd_head_" + options.headLimit(), options.file(), options.headLimit()));
            return;
        }

//        if (options.datasetOption().isPpd() && options.engineOption() != EngineOption.DUCKDB) {
//            throw new IllegalArgumentException("PPD dataset is currently supported only with --engine duckdb");
//        }

        if (options.engineOption().includesParquet() && isCsv(options.file())) {
            System.err.println("Parquet engine cannot read CSV input directly. Run --to-parquet first.");
            throw new IllegalStateException("CSV input not supported by Parquet engine");
        }

        List<QueryCase> cases = caseSelector.select(options.datasetOption(), options.caseFilter(), options.file());
        if (cases.isEmpty()) {
            throw new IllegalStateException("No cases selected");
        }
        List<AnalyticsEngine> engines = engineFactory.create(options.engineOption());
        System.out.println("Dataset=" + options.datasetOption());
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

    private void runInteractive(RunnerOptions options, QueryCase queryCase) throws Exception {
        System.out.printf("Running %s (%s) with DuckDB...%n", queryCase.id(), CaseDescriptions.describe(queryCase));
        RunConfig config = new RunConfig(
                0,
                1,
                options.threads(),
                options.limitRows(),
                options.explain(),
                false
        );
        new DuckdbEngine().runInteractive(queryCase, config);
    }

    private void runCsvToParquet(RunnerOptions options) throws Exception {
        ensureDuckdbEngine(options.engineOption());
        String out = options.toParquet();
        if (out == null || out.isBlank()) {
            throw new IllegalArgumentException("--to-parquet requires an output file path");
        }
        System.out.printf("Converting %s -> %s ...%n", options.file(), out);
        Stopwatch stopwatch = Stopwatch.createStarted();
        new DuckdbEngine().csvToParquet(options.file(), out, options.threads());
        stopwatch.stop();
        System.out.printf("Parquet written to %s in %d ms%n", out, stopwatch.elapsedMillis());
    }

    private static void ensureDuckdbEngine(EngineOption engineOption) {
        if (!engineOption.includesDuckdb()) {
            throw new IllegalArgumentException("This command requires --engine duckdb");
        }
    }

    private static boolean isCsv(String file) {
        return file != null && file.toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    record BenchResult(QueryCase queryCase, String engine, CaseResult result) {
    }
}
