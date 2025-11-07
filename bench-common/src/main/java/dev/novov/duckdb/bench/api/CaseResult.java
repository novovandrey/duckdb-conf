package dev.novov.duckdb.bench.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public record CaseResult(
        QueryCase queryCase,
        String engine,
        List<CaseRun> runs
) {
    public CaseResult {
        runs = List.copyOf(runs);
    }

    public long medianMillis() {
        if (runs.isEmpty()) {
            return 0L;
        }
        List<Long> copy = new ArrayList<>(runs.size());
        for (CaseRun run : runs) {
            copy.add(run.nanos());
        }
        copy.sort(Comparator.naturalOrder());
        int mid = copy.size() / 2;
        long nanos;
        if (copy.size() % 2 == 0) {
            nanos = (copy.get(mid - 1) + copy.get(mid)) / 2;
        } else {
            nanos = copy.get(mid);
        }
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    public List<CaseRun> runs() {
        return Collections.unmodifiableList(runs);
    }
}
