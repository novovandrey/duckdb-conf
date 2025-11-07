package dev.novov.duckdb.bench.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public final class MemoryUtil {
    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();

    private MemoryUtil() {
    }

    public static long sampleUsedBytes() {
        return used(MEMORY_BEAN.getHeapMemoryUsage()) + used(MEMORY_BEAN.getNonHeapMemoryUsage());
    }

    public static long measureDeltaBytes(Runnable runnable) {
        long before = sampleUsedBytes();
        runnable.run();
        long after = sampleUsedBytes();
        return Math.max(0L, after - before);
    }

    private static long used(MemoryUsage usage) {
        return usage == null ? 0L : usage.getUsed();
    }
}
