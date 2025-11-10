package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.NewBuildVsOldCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.NewBuildVsOldCase;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class NewVsOldExecutor {

    public CaseRun execute(NewBuildVsOldCase query, RunConfig cfg) throws Exception {
        final String file = query.file();
        final long t0 = System.nanoTime();

        // key: is_new_build (true/false)
        Map<Boolean, Agg> byIsNew = new HashMap<>(4);

        Configuration conf = new Configuration(false);
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, """
            message ppd {
              optional binary new_build (UTF8);
              optional int64 price;
              optional binary ppd_category (UTF8);
            }
            """);

        long scanned = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(conf).build()) {
            Group g;
            while ((g = reader.read()) != null) {
                final String cat = optStr(g, "ppd_category");
                if (!"A".equals(cat)) continue;

                final String nb = optStr(g, "new_build");
                final boolean isNew = "Y".equals(nb); // читаемая метка
                final long price = optLong(g, "price", Long.MIN_VALUE);
                if (price == Long.MIN_VALUE) continue;

                byIsNew.computeIfAbsent(isNew, k -> new Agg()).add(price);
                scanned++;
            }
        }

        final long rowsOut = byIsNew.size(); // 1..2
        final long nanos = System.nanoTime() - t0;
        final long maxMem = currentUsedMemApprox();

        return new CaseRun(nanos, rowsOut, -1, maxMem);
    }

    private static final class Agg {
        long n;
        long sum;
        void add(long p) { n++; sum += p; }
        double avg() { return n == 0 ? 0.0 : (double) sum / n; }
    }

    private static String optStr(Group g, String f) {
        return g.getFieldRepetitionCount(f) == 0 ? null : g.getBinary(f, 0).toStringUsingUTF8();
    }

    private static long optLong(Group g, String f, long def) {
        return g.getFieldRepetitionCount(f) == 0 ? def : g.getLong(f, 0);
    }

    private static long currentUsedMemApprox() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
