package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.MemoryUtil;
import dev.novov.duckdb.bench.util.Stopwatch;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FilterExecutor {
    private static final Pattern BETWEEN_PATTERN = Pattern.compile("(?i)([\\w.]+)\\s+between\\s+'([^']+)'\\s+and\\s+'([^']+)'" );
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("(?i)([\\w.]+)\\s*(>=|<=|>|<|=)\\s*([0-9]+(?:\\.[0-9]+)?)");

    CaseRun execute(FilterCase filterCase, RunConfig config) throws IOException {
        ParsedFilter parsed = ParsedFilter.parse(filterCase.filterExpr());
        Stopwatch stopwatch = Stopwatch.createStarted();
        long before = MemoryUtil.sampleUsedBytes();
        final long[] matched = {0L};

        try (ParquetScanner scanner = ParquetScanner.open(filterCase.file(), List.of(parsed.column()))) {
            GroupPredicate predicate = parsed.asPredicate(scanner);
            scanner.scan(group -> {
                if (predicate.test(group)) {
                    matched[0]++;
                }
            }, config.limitRowsOrMinusOne());
        }

        stopwatch.stop();
        long after = MemoryUtil.sampleUsedBytes();
        long deltaMem = Math.max(0L, after - before);
        return new CaseRun(stopwatch.elapsedNanos(), matched[0], -1L, deltaMem);
    }

    private interface GroupPredicate {
        boolean test(Group group);
    }

    private record ParsedFilter(String column,
                                FilterKind kind,
                                String lower,
                                String upper,
                                String operator,
                                double numericValue) {
        static ParsedFilter parse(String raw) {
            String expression = raw.trim();
            Matcher between = BETWEEN_PATTERN.matcher(expression);
            if (between.matches()) {
                return new ParsedFilter(between.group(1).trim(), FilterKind.BETWEEN,
                        between.group(2), between.group(3), null, 0d);
            }
            Matcher numeric = NUMERIC_PATTERN.matcher(expression);
            if (numeric.matches()) {
                double value = Double.parseDouble(numeric.group(3));
                return new ParsedFilter(numeric.group(1).trim(), FilterKind.NUMERIC,
                        null, null, numeric.group(2), value);
            }
            throw new IllegalArgumentException("Unsupported filter expression: " + raw);
        }

        GroupPredicate asPredicate(ParquetScanner scanner) {
            return switch (kind) {
                case BETWEEN -> group -> {
                    String value = scanner.getString(group, column);
                    if (value == null) {
                        return false;
                    }
                    String normalized = value.toUpperCase(Locale.ROOT);
                    return normalized.compareTo(lower.toUpperCase(Locale.ROOT)) >= 0
                            && normalized.compareTo(upper.toUpperCase(Locale.ROOT)) <= 0;
                };
                case NUMERIC -> group -> {
                    double candidate = scanner.getDouble(group, column);
                    if (Double.isNaN(candidate)) {
                        return false;
                    }
                    return switch (operator) {
                        case ">" -> candidate > numericValue;
                        case ">=" -> candidate >= numericValue;
                        case "<" -> candidate < numericValue;
                        case "<=" -> candidate <= numericValue;
                        case "=" -> candidate == numericValue;
                        default -> throw new IllegalStateException("Unexpected operator " + operator);
                    };
                };
            };
        }
    }

    private enum FilterKind {
        BETWEEN,
        NUMERIC
    }
}
