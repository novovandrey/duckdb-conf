package dev.novov.duckdb.engines.parquet;
import java.lang.management.*;
import java.util.List;

public class JvmMemoryStats {

    public static final class Stats {
        public final long heapUsed;
        public final long nonHeapUsed;
        public final long directUsed;
        public final long mappedUsed;

        public Stats(long heapUsed, long nonHeapUsed, long directUsed, long mappedUsed) {
            this.heapUsed = heapUsed;
            this.nonHeapUsed = nonHeapUsed;
            this.directUsed = directUsed;
            this.mappedUsed = mappedUsed;
        }

        public long totalApproxAllocated() {
            return heapUsed + nonHeapUsed + directUsed + mappedUsed;
        }
    }

    public static Stats collect() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

        long heapUsed = mem.getHeapMemoryUsage().getUsed();
        long nonHeapUsed = mem.getNonHeapMemoryUsage().getUsed();

        long directUsed = 0L;
        long mappedUsed = 0L;

        for (BufferPoolMXBean bp : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equalsIgnoreCase(bp.getName())) {
                directUsed += bp.getMemoryUsed();
            } else if ("mapped".equalsIgnoreCase(bp.getName())) {
                mappedUsed += bp.getMemoryUsed();
            }
        }

        return new Stats(heapUsed, nonHeapUsed, directUsed, mappedUsed);
    }

}
