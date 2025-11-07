package dev.novov.duckdb.bench.util;

import java.util.concurrent.TimeUnit;

public final class Stopwatch {
    private long startNanos;
    private long endNanos;
    private boolean running;

    private Stopwatch() {
    }

    public static Stopwatch createStarted() {
        return new Stopwatch().start();
    }

    public Stopwatch start() {
        if (!running) {
            running = true;
            startNanos = System.nanoTime();
        }
        return this;
    }

    public Stopwatch stop() {
        if (running) {
            endNanos = System.nanoTime();
            running = false;
        }
        return this;
    }

    public boolean isRunning() {
        return running;
    }

    public long elapsedNanos() {
        return running ? System.nanoTime() - startNanos : endNanos - startNanos;
    }

    public long elapsedMillis() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanos());
    }
}
