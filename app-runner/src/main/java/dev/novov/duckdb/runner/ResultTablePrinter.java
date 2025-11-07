package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.util.AsciiTable;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

final class ResultTablePrinter {
    private ResultTablePrinter() {
    }

    static void print(List<BenchRunner.BenchResult> results, int warmups, int runs) {
        if (results.isEmpty()) {
            System.out.println("No results to display");
            return;
        }
        results.sort(Comparator.comparing((BenchRunner.BenchResult r) -> r.queryCase().id())
                .thenComparing(BenchRunner.BenchResult::engine));
        AsciiTable table = new AsciiTable(List.of("Case", "Engine", "Runs", "Median(ms)", "RowsOut", "Rows/s", "MaxMem(MB)"));
        for (BenchRunner.BenchResult result : results) {
            long medianMillis = result.result().medianMillis();
            long rowsOut = rowsOut(result);
            String rowsPerSecond = rowsPerSecond(result.queryCase(), rowsOut, medianMillis);
            String memMb = formatMem(toMaxMemBytes(result));
            table.addRow(
                    result.queryCase().id(),
                    result.engine(),
                    warmups + "/" + runs,
                    formatMillis(medianMillis),
                    formatNumber(rowsOut),
                    rowsPerSecond,
                    memMb
            );
        }
        System.out.println(table.render());
    }

    private static long rowsOut(BenchRunner.BenchResult result) {
        return result.result().runs().stream()
                .mapToLong(CaseRun::rowsOut)
                .max()
                .orElse(0L);
    }

    private static long toMaxMemBytes(BenchRunner.BenchResult result) {
        return result.result().runs().stream()
                .mapToLong(CaseRun::maxUsedMemBytes)
                .max()
                .orElse(0L);
    }

    private static String rowsPerSecond(QueryCase queryCase, long rowsOut, long medianMillis) {
        if (rowsOut <= 0 || medianMillis <= 0) {
            return "—";
        }
        if (queryCase instanceof dev.novov.duckdb.bench.api.TopKCase) {
            return "—";
        }
        double perSecond = rowsOut / (medianMillis / 1000d);
        return formatNumber(perSecond);
    }

    private static String formatMillis(long millis) {
        return String.format(Locale.US, "%,d", millis);
    }

    private static String formatNumber(double value) {
        if (value == 0d) {
            return "0";
        }
        double abs = Math.abs(value);
        if (abs >= 1000d) {
            return String.format(Locale.US, "%.1e", value);
        }
        return String.format(Locale.US, "%.2f", value);
    }

    private static String formatNumber(long value) {
        if (value >= 1000) {
            return String.format(Locale.US, "%.1e", (double) value);
        }
        return Long.toString(value);
    }

    private static String formatMem(long bytes) {
        double mb = bytes / (1024d * 1024d);
        return String.format(Locale.US, "%.0f", mb);
    }
}
