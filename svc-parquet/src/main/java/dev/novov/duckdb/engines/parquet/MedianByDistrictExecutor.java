package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.MedianByDistrictCase;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.*;
import java.util.stream.Collectors;

public class MedianByDistrictExecutor {
    public CaseRun execute(MedianByDistrictCase query, RunConfig cfg) throws Exception {
        final String file = query.file();
        final int minCount = query.minCount();
        final int limit = query.limit();
        final long t0 = System.nanoTime();

        Map<String, Long> counts = new HashMap<>(1 << 14);

        Configuration conf1 = new Configuration(false);
        conf1.set(ReadSupport.PARQUET_READ_SCHEMA, """
                message ppd {
                  optional binary district (UTF8);
                  optional binary ppd_category (UTF8);
                }
                """);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(conf1).build()) {
            Group g;
            while ((g = reader.read()) != null) {
                final String cat = optStr(g, "ppd_category");
                if (!"A".equals(cat)) continue;
                final String district = optStr(g, "district");
                if (district == null || district.isEmpty()) continue;
                counts.merge(district, 1L, Long::sum);
            }
        }

        // choose heavy districts (HAVING n > minCount)
        Set<String> heavy = counts.entrySet().stream()
                .filter(e -> e.getValue() != null && e.getValue() > minCount)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        if (heavy.isEmpty()) {
            final long nanos = System.nanoTime() - t0;
            final long maxMem = currentUsedMemApprox();
            return new CaseRun(nanos, 0, -1, maxMem);
        }

        Map<String, List<Long>> prices = new HashMap<>(heavy.size() * 2);

        Configuration conf2 = new Configuration(false);
        conf2.set(ReadSupport.PARQUET_READ_SCHEMA, """
                message ppd {
                  optional binary district (UTF8);
                  optional int64 price;
                  optional binary ppd_category (UTF8);
                }
                """);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(conf2).build()) {
            Group g;
            while ((g = reader.read()) != null) {
                final String cat = optStr(g, "ppd_category");
                if (!"A".equals(cat)) continue;
                final String district = optStr(g, "district");
                if (district == null || district.isEmpty() || !heavy.contains(district)) continue;
                final long price = optLong(g, "price", Long.MIN_VALUE);
                if (price == Long.MIN_VALUE) continue;
                prices.computeIfAbsent(district, k -> new ArrayList<>()).add(price);
            }
        }

        // Compute median per district
        List<Row> out = new ArrayList<>(prices.size());
        for (Map.Entry<String, List<Long>> e : prices.entrySet()) {
            List<Long> arr = e.getValue();
            if (arr.isEmpty()) continue;
            long median = medianInPlace(arr);
            out.add(new Row(e.getKey(), median, arr.size()));
        }

        // ORDER BY median DESC LIMIT <limit>
        out.sort((a, b) -> Long.compare(b.median, a.median));
        if (limit > 0 && out.size() > limit) {
            out = out.subList(0, limit);
        }

        final long rowsOut = out.size();
        final long nanos = System.nanoTime() - t0;
        final long maxMem = currentUsedMemApprox();

        return new CaseRun(nanos, rowsOut, -1, maxMem);
    }

    private static long medianInPlace(List<Long> a) {
        a.sort(Long::compare);
        int n = a.size();
        if (n % 2 == 1) return a.get(n / 2);
        long x = a.get(n / 2 - 1), y = a.get(n / 2);
        return x + ((y - x) / 2);
    }

    private static String optStr(Group g, String f) {
        return g.getFieldRepetitionCount(f) == 0 ? null : g.getBinary(f, 0).toStringUsingUTF8();
    }

    private static long optLong(Group g, String f, long def) {
        return g.getFieldRepetitionCount(f) == 0 ? def : g.getLong(f, 0);
    }

    private record Row(String district, long median, int n) {
    }

    private static long currentUsedMemApprox() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
