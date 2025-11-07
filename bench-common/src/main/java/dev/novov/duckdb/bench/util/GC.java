package dev.novov.duckdb.bench.util;

import java.time.Duration;

public final class GC {
    private GC() {
    }

    public static void requestGc(Duration pause) {
        System.gc();
        if (pause.isNegative() || pause.isZero()) {
            return;
        }
        try {
            Thread.sleep(pause.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
