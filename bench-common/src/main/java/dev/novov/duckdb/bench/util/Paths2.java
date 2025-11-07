package dev.novov.duckdb.bench.util;

import java.nio.file.Paths;
import java.util.Locale;

public final class Paths2 {
    private Paths2() {
    }

    /** Normalize local paths to forward slashes; keep URLs intact. */
    public static String normalizePathOrUrl(String raw) {
        if (raw == null) {
            return null;
        }
        String lower = raw.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://") || lower.startsWith("file:/")) {
            return raw;
        }
        return Paths.get(raw).toAbsolutePath().toString().replace('\\', '/');
    }
}
