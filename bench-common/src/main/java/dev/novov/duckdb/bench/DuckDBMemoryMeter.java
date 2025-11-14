package dev.novov.duckdb.bench;

import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import java.lang.management.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class DuckDBMemoryMeter {

    public static Result measurePeakDuring(Runnable workload, Duration samplePeriod) throws Exception {
        Snapshot base = snapshot();
        long baselineNonJvm = Math.max(0, base.rss - base.jvmUsedTotal());

        AtomicLong peakDuckdb = new AtomicLong(0);
        AtomicLong peakRss = new AtomicLong(0);
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mem-sampler");
            t.setDaemon(true);
            return t;
        });

        Future<?> task = Executors.newSingleThreadExecutor().submit(() -> {
            try { workload.run(); } catch (Throwable t) { throw new RuntimeException(t); }
        });

        ScheduledFuture<?> sampler = ses.scheduleAtFixedRate(() -> {
            Snapshot s = snapshot();
            peakRss.accumulateAndGet(s.rss(), Math::max);
            long nonJvmNow = Math.max(0, s.rss() - s.jvmUsedTotal());
            long duckNow = Math.max(0, nonJvmNow - baselineNonJvm);
            peakDuckdb.accumulateAndGet(duckNow, Math::max);
        }, 0, samplePeriod.toMillis(), TimeUnit.MILLISECONDS);

        try {
            task.get();
        } finally {
            sampler.cancel(true);
            ses.shutdownNow();
        }

        Snapshot end = snapshot();
        long nonJvmEnd = Math.max(0, end.rss - end.jvmUsedTotal());
        long duckNow = Math.max(0, nonJvmEnd - baselineNonJvm);

        return new Result(peakRss.get(), baselineNonJvm, peakDuckdb.get(), duckNow, end);
    }

    public record Snapshot(long rss, long heapUsed, long nonHeapUsed, long directUsed, long mappedUsed) {
        long jvmUsedTotal() { return heapUsed + nonHeapUsed + directUsed + mappedUsed; }
    }

    public record Result(long peakRss, long baselineNonJvm, long peakDuckdbApprox, long currentDuckdbApprox, Snapshot last) {
        @Override public String toString() {
            return """
                   baselineNonJvm        = %,d
                   peakDuckdbApprox      = %,d
                   currentDuckdbApprox   = %,d
                   ---- last snapshot (bytes) ----
                   rss                   = %,d
                   jvm.heapUsed          = %,d
                   jvm.nonHeapUsed       = %,d
                   jvm.directUsed        = %,d
                   jvm.mappedUsed        = %,d
                   jvm.totalUsed         = %,d
                   """.formatted(
                    baselineNonJvm, peakDuckdbApprox, currentDuckdbApprox,
                    last.rss, last.heapUsed, last.nonHeapUsed, last.directUsed, last.mappedUsed,
                    last.jvmUsedTotal()
            );
        }
    }

    private static final SystemInfo SI = new SystemInfo();
    private static final OperatingSystem OS = SI.getOperatingSystem();
    private static final int PID = OS.getProcessId();

    private static Snapshot snapshot() {
        long rss = currentRss();
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

        long heap = mem.getHeapMemoryUsage().getUsed();
        long nonHeap = mem.getNonHeapMemoryUsage().getUsed();

        long direct = 0, mapped = 0;
        for (BufferPoolMXBean bp : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            String n = bp.getName();
            long used = safe(bp::getMemoryUsed);
            if ("direct".equalsIgnoreCase(n)) direct += used;
            else if ("mapped".equalsIgnoreCase(n)) mapped += used;
        }
        return new Snapshot(rss, heap, nonHeap, direct, mapped);
    }

    private static long currentRss() {
        OSProcess p = OS.getProcess(PID);
        return p == null ? 0 : p.getResidentSetSize();
    }

    private static long safe(LongSupplierEx s) {
        try { return s.getAsLong(); } catch (Throwable t) { return 0; }
    }

    @FunctionalInterface private interface LongSupplierEx { long getAsLong() throws Exception; }

}
