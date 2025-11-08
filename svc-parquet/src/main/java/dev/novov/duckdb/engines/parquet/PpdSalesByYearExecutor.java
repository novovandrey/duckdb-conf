package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.GroupByYearCase;
import dev.novov.duckdb.bench.api.RunConfig;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class PpdSalesByYearExecutor {

    public CaseRun execute(GroupByYearCase query, RunConfig cfg) throws Exception {
        final String file = query.file();
        final long t0 = System.nanoTime();

        Map<Integer, Agg> byYear = new HashMap<>(256);

        Configuration conf = new Configuration(false);
        conf.setBoolean("parquet.filter.statistics.enabled", true);
        conf.setBoolean("parquet.filter.dictionary.enabled", true);
        conf.setBoolean("parquet.filter.columnindex.enabled", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, """
            message ppd {
              optional int32 transfer_date (DATE);
              optional int64 price;
              optional binary ppd_category (UTF8);
            }
            """);

        long rows = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(conf).build()) {
            Group g;
            while ((g = reader.read()) != null) {
                final String cat = optStr(g, "ppd_category");
                if (!"A".equals(cat)) continue;

                final int days = optInt(g, "transfer_date", Integer.MIN_VALUE);
                if (days == Integer.MIN_VALUE) continue;
                final int year = (int) LocalDate.ofEpochDay(days).getYear();

                final long price = optLong(g, "price", Long.MIN_VALUE);
                if (price == Long.MIN_VALUE) continue;

                byYear.computeIfAbsent(year, k -> new Agg()).add(price);
                rows++;
            }
        }

        final long rowsOut = byYear.size();
        final long nanos = System.nanoTime() - t0;
        final long maxMem = currentUsedMemApprox();

        return new CaseRun(nanos, rowsOut, -1, maxMem);
    }

    private static final class Agg {
        long n;
        long sum;
        void add(long price) { n++; sum += price; }
        double avg() { return n == 0 ? 0.0 : (double) sum / n; }
    }

    private static String optStr(Group g, String f) {
        return g.getFieldRepetitionCount(f) == 0 ? null : g.getBinary(f, 0).toStringUsingUTF8();
        // NOTE: parquet-example Group API: getBinary(...).toStringUsingUTF8()
    }

    private static long optLong(Group g, String f, long def) {
        return g.getFieldRepetitionCount(f) == 0 ? def : g.getLong(f, 0);
    }

    private static int optInt(Group g, String f, int def) {
        return g.getFieldRepetitionCount(f) == 0 ? def : g.getInteger(f, 0);
    }

    private static long currentUsedMemApprox() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
