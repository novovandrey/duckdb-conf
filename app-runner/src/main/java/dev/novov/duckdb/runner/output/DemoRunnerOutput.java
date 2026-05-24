package dev.novov.duckdb.runner.output;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.runner.BenchRunner;
import dev.novov.duckdb.runner.DemoResultTablePrinter;
import dev.novov.duckdb.runner.RunnerOptions;
import dev.novov.duckdb.runner.demo.DemoCatalog;
import dev.novov.duckdb.runner.terminal.TerminalBox;
import dev.novov.duckdb.runner.terminal.TerminalLine;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class DemoRunnerOutput implements RunnerOutput {
    private DemoCatalog.DemoMeta activeMeta;

    @Override
    public void printRunHeader(RunnerOptions options, List<QueryCase> cases, List<AnalyticsEngine> engines) {
        QueryCase first = cases.getFirst();
        activeMeta = DemoCatalog.resolve(first);
        List<TerminalLine> lines = new ArrayList<>();
        lines.add(new TerminalLine("Goal", activeMeta.goal()));
        lines.add(new TerminalLine("Engine", engines.stream().map(AnalyticsEngine::name).collect(Collectors.joining(", "))));
        lines.add(new TerminalLine("Dataset", String.valueOf(options.datasetOption()).toLowerCase()));
        lines.add(new TerminalLine("Threads", Integer.toString(options.threads())));
        System.out.println(TerminalBox.render("Demo " + activeMeta.number() + " / " + activeMeta.title(), TerminalBox.keyValues(lines), 78));
        System.out.println();
        System.out.println("Running...");
        System.out.println();
    }

    @Override
    public void printCaseStarted(QueryCase queryCase, AnalyticsEngine engine) {
    }

    @Override
    public void printCaseFinished(QueryCase queryCase, String engineName, CaseResult result) {
        long rows = result.runs().stream().mapToLong(CaseRun::rowsOut).max().orElse(0L);
        System.out.println("✓ " + queryCase.id() + " on " + engineName + " completed");
        System.out.println("✓ Median time: " + result.medianMillis() + " ms");
        System.out.println("✓ Result rows: " + rows);
        System.out.println();
    }

    @Override
    public void printRunFinished(List<BenchRunner.BenchResult> results, int warmups, int runs) {
        DemoResultTablePrinter.print(results);
        if (activeMeta != null) {
            System.out.println();
            System.out.println("Takeaway:");
            System.out.println(activeMeta.takeaway());
        }
    }

    @Override
    public void printInfo(String message) {
        System.out.println(message);
    }

    @Override
    public void printError(String message) {
        System.err.println("✗ Demo failed");
        System.err.println();
        System.err.println("Reason:");
        System.err.println(message);
    }
}
