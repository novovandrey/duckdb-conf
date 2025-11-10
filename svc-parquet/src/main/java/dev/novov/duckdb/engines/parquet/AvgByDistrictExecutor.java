package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.AvgByDistrictCase;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class AvgByDistrictExecutor {

    public CaseRun execute(AvgByDistrictCase query, RunConfig cfg) throws Exception {
        final String file = query.file();
        final int minCount = query.minCount();
        final long t0 = System.nanoTime();

        Map<String, Agg> byDistrict = new HashMap<>(1 << 14);

        Configuration conf = new Configuration(false);
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, """
            message ppd {
              optional binary district (UTF8);
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

                final String district = optStr(g, "district");
                if (district == null || district.isEmpty()) continue;

                final long price = optLong(g, "price", Long.MIN_VALUE);
                if (price == Long.MIN_VALUE) continue;

                byDistrict.computeIfAbsent(district, k -> new Agg()).add(price);
                scanned++;
            }
        }

        // HAVING n > minCount
        List<Map.Entry<String, Agg>> rows = new ArrayList<>(byDistrict.entrySet());
        rows.removeIf(e -> e.getValue().n <= minCount);
        rows.sort((a, b) -> Double.compare(b.getValue().avg(), a.getValue().avg())); // DESC by avg

        final long rowsOut = rows.size();
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
