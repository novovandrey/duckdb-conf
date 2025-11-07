package dev.novov.duckdb.bench.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Stats {
    private Stats() {
    }

    public static double median(List<Long> values) {
        return percentile(values, 0.5d);
    }

    public static double p95(List<Long> values) {
        return percentile(values, 0.95d);
    }

    private static double percentile(List<Long> values, double percentile) {
        if (values == null || values.isEmpty()) {
            return 0d;
        }
        List<Long> copy = new ArrayList<>(values);
        Collections.sort(copy);
        double index = percentile * (copy.size() - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return copy.get(lower);
        }
        double weight = index - lower;
        return copy.get(lower) * (1 - weight) + copy.get(upper) * weight;
    }
}
