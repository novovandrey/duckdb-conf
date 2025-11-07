package dev.novov.duckdb.bench.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Files2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(Files2.class);

    private Files2() {
    }

    public static Path resolveLocal(String candidate) {
        if (isHttp(candidate)) {
            throw new IllegalArgumentException("Expected local path but got URL: " + candidate);
        }
        return Path.of(candidate).toAbsolutePath().normalize();
    }

    public static Path requireLocalFile(String candidate) {
        Path path = resolveLocal(candidate);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File does not exist: " + path);
        }
        if (!Files.isReadable(path)) {
            throw new IllegalArgumentException("File is not readable: " + path);
        }
        return path;
    }

    public static boolean exists(String location) {
        if (isHttp(location)) {
            return probeHttp(location);
        }
        return Files.exists(Path.of(location));
    }

    private static boolean probeHttp(String location) {
        try {
            URL url = new URL(location);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(3_000);
            connection.setReadTimeout(3_000);
            int code = connection.getResponseCode();
            LOGGER.info("Probed {} -> status {}", location, code);
            return code >= 200 && code < 400;
        } catch (IOException ex) {
            LOGGER.warn("Unable to probe {}: {}", location, ex.getMessage());
            return false;
        }
    }

    private static boolean isHttp(String location) {
        return location.startsWith("http://") || location.startsWith("https://");
    }
}
