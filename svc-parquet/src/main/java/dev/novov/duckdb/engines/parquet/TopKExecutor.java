package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.TopKCase;
import dev.novov.duckdb.bench.util.MemoryUtil;
import dev.novov.duckdb.bench.util.Stopwatch;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

final class TopKExecutor {
    CaseRun execute(TopKCase topKCase, RunConfig config) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long before = MemoryUtil.sampleUsedBytes();
        int k = Math.max(1, topKCase.k());
        Comparator<Double> comparator = topKCase.desc() ? Comparator.naturalOrder() : Comparator.reverseOrder();
        PriorityQueue<Double> heap = new PriorityQueue<>(k, comparator);
        final long[] seen = {0L};

        try (ParquetScanner scanner = ParquetScanner.open(topKCase.file(), List.of(topKCase.orderByCol()))) {
            scanner.scan(group -> {
                double value = scanner.getDouble(group, topKCase.orderByCol());
                if (Double.isNaN(value)) {
                    return;
                }
                seen[0]++;
                if (heap.size() < k) {
                    heap.add(value);
                    return;
                }
                Double head = heap.peek();
                if (head == null) {
                    return;
                }
                if (topKCase.desc()) {
                    if (value > head) {
                        heap.poll();
                        heap.add(value);
                    }
                } else {
                    if (value < head) {
                        heap.poll();
                        heap.add(value);
                    }
                }
            }, config.limitRowsOrMinusOne());
        }

        stopwatch.stop();
        long after = MemoryUtil.sampleUsedBytes();
        long deltaMem = Math.max(0L, after - before);
        long rowsOut = Math.min(seen[0], k);
        return new CaseRun(stopwatch.elapsedNanos(), rowsOut, -1L, deltaMem);
    }
}
