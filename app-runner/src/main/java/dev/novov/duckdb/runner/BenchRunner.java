package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.DescribeCase;
import dev.novov.duckdb.bench.api.HeadCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.Stopwatch;
import dev.novov.duckdb.engines.DuckdbEngine;
import dev.novov.duckdb.runner.output.DemoRunnerOutput;
import dev.novov.duckdb.runner.output.PlainRunnerOutput;
import dev.novov.duckdb.runner.output.RunnerOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class BenchRunner {
    private final EngineFactory engineFactory = new EngineFactory();
    private final CaseSelector caseSelector = new CaseSelector();

    void run(RunnerOptions options) throws Exception {
        RunnerOutput output = options.demoMode() ? new DemoRunnerOutput() : new PlainRunnerOutput();
        if (options.toParquet() != null) {
            runCsvToParquet(options, output);
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
            output.printError("Parquet engine cannot read CSV input directly. Run --to-parquet first.");
            throw new IllegalStateException("CSV input not supported by Parquet engine");
        }

        List<QueryCase> cases = caseSelector.select(options.datasetOption(), options.caseFilter(), options.file());
        if (cases.isEmpty()) {
            throw new IllegalStateException("No cases selected");
        }
        List<AnalyticsEngine> engines = engineFactory.create(options.engineOption());
        output.printRunHeader(options, cases, engines);

        RunConfig config = new RunConfig(
                options.warmups(),
                options.runs(),
                options.threads(),
                options.limitRows(),
                options.explain(),
                options.explainHtml(),
                false
        );

        List<BenchResult> results = new ArrayList<>();
        for (QueryCase queryCase : cases) {
            for (AnalyticsEngine engine : engines) {
                output.printCaseStarted(queryCase, engine);
                CaseResult result = engine.run(queryCase, config);
                results.add(new BenchResult(queryCase, engine.name(), result));
                output.printCaseFinished(queryCase, engine.name(), result);
            }
        }

        output.printRunFinished(results, options.warmups(), options.runs());
    }

    private void runInteractive(RunnerOptions options, QueryCase queryCase) throws Exception {
        System.out.printf("Running %s (%s) with DuckDB...%n", queryCase.id(), CaseDescriptions.describe(queryCase));
        RunConfig config = new RunConfig(
                0,
                1,
                options.threads(),
                options.limitRows(),
                options.explain(),
                options.explainHtml(),
                false
        );
        new DuckdbEngine().runInteractive(queryCase, config);
    }

    private void runCsvToParquet(RunnerOptions options, RunnerOutput output) throws Exception {
        ensureDuckdbEngine(options.engineOption());
        String out = options.toParquet();
        if (out == null || out.isBlank()) {
            throw new IllegalArgumentException("--to-parquet requires an output file path");
        }
        output.printInfo("Converting " + options.file() + " -> " + out + " ...");
        Stopwatch stopwatch = Stopwatch.createStarted();
        new DuckdbEngine().csvToParquet(options.file(), out, options.threads());
        stopwatch.stop();
        output.printInfo("Parquet written to " + out + " in " + stopwatch.elapsedMillis() + " ms");
    }

    private static void ensureDuckdbEngine(EngineOption engineOption) {
        if (!engineOption.includesDuckdb()) {
            throw new IllegalArgumentException("This command requires --engine duckdb");
        }
    }

    private static boolean isCsv(String file) {
        return file != null && file.toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    public record BenchResult(QueryCase queryCase, String engine, CaseResult result) {
    }
}
