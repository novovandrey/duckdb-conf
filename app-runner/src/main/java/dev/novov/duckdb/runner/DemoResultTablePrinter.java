package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.runner.terminal.TerminalBox;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public final class DemoResultTablePrinter {
    private DemoResultTablePrinter() {
    }

    public static void print(List<BenchRunner.BenchResult> results) {
        List<String> rows = new ArrayList<>();
        rows.add(String.format("%-12s %-12s %-10s %-10s", "Engine", "Median(ms)", "RowsOut", "MaxMem(MB)"));
        results.stream()
                .sorted(Comparator.comparing(BenchRunner.BenchResult::engine))
                .forEach(result -> rows.add(String.format("%-12s %-12d %-10d %-10s",
                        result.engine(),
                        result.result().medianMillis(),
                        rowsOut(result),
                        formatMem(maxMemBytes(result)))));
        System.out.println(TerminalBox.render("Result", rows, 78));
    }

    private static long rowsOut(BenchRunner.BenchResult result) {
        return result.result().runs().stream().mapToLong(CaseRun::rowsOut).max().orElse(0L);
    }

    private static long maxMemBytes(BenchRunner.BenchResult result) {
        return result.result().runs().stream().mapToLong(CaseRun::maxUsedMemBytes).max().orElse(0L);
    }

    private static String formatMem(long bytes) {
        return String.format(Locale.US, "%.0f", bytes / (1024d * 1024d));
    }
}
