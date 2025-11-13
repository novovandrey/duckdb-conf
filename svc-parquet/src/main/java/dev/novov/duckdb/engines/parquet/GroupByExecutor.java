package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.AggFn;
import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.GroupByCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.MemoryUtil;
import dev.novov.duckdb.bench.util.Stopwatch;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.io.IOException;
import java.util.List;

final class GroupByExecutor {
    CaseRun execute(GroupByCase groupByCase, RunConfig config) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long before = MemoryUtil.sampleUsedBytes();

        Object2LongOpenHashMap<String> counts = new Object2LongOpenHashMap<>();
        counts.defaultReturnValue(0L);
        Object2DoubleOpenHashMap<String> sums = new Object2DoubleOpenHashMap<>();
        sums.defaultReturnValue(0d);

        try (ParquetScanner scanner = ParquetScanner.open(
                groupByCase.file(),
                List.of(groupByCase.groupCol(), groupByCase.aggCol())
        )) {
            scanner.scan(group -> {
                String key = scanner.getKey(group, groupByCase.groupCol());
                counts.addTo(key, 1L);
                double value = scanner.getDouble(group, groupByCase.aggCol());
                if (!Double.isNaN(value)) {
                    sums.addTo(key, value);
                }
            }, config.limitRowsOrMinusOne());
        }

        stopwatch.stop();
        long after = MemoryUtil.sampleUsedBytes();
        long rowsOut = counts.size();
        long deltaMem = Math.max(0L, after - before);

        long deltaMem2 = MemoryUtil.sampleUsedBytesV2();
        if (groupByCase.aggFn() == AggFn.AVG) {
            // Force evaluation to mimic the cost of producing output rows.
            counts.object2LongEntrySet().fastForEach(entry -> {
                double sum = sums.getDouble(entry.getKey());
                double avg = entry.getLongValue() == 0 ? 0d : sum / entry.getLongValue();
                // no-op sink to keep JVM from optimizing away.
                if (Double.isInfinite(avg)) {
                    throw new IllegalStateException("Average overflow");
                }
            });
        }

        return new CaseRun(stopwatch.elapsedNanos(), rowsOut, -1L, deltaMem2);
    }
}
