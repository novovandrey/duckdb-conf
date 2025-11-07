package dev.novov.duckdb.bench.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class AsciiTable {
    private final List<String> headers;
    private final List<List<String>> rows = new ArrayList<>();

    public AsciiTable(List<String> headers) {
        if (headers == null || headers.isEmpty()) {
            throw new IllegalArgumentException("Headers must be provided");
        }
        this.headers = List.copyOf(headers);
    }

    public AsciiTable addRow(Object... values) {
        if (values.length != headers.size()) {
            throw new IllegalArgumentException("Expected " + headers.size() + " values but got " + values.length);
        }
        rows.add(Arrays.stream(values).map(value -> Objects.toString(value, "")).toList());
        return this;
    }

    public String render() {
        int columns = headers.size();
        int[] widths = new int[columns];
        for (int i = 0; i < columns; i++) {
            widths[i] = headers.get(i).length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < columns; i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        String horizontal = line(widths);
        StringBuilder sb = new StringBuilder();
        sb.append(horizontal).append(System.lineSeparator());
        sb.append(renderRow(headers, widths)).append(System.lineSeparator());
        sb.append(horizontal).append(System.lineSeparator());
        for (List<String> row : rows) {
            sb.append(renderRow(row, widths)).append(System.lineSeparator());
        }
        sb.append(horizontal);
        return sb.toString();
    }

    private static String renderRow(List<String> row, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < row.size(); i++) {
            sb.append(' ').append(padRight(row.get(i), widths[i])).append(' ').append('|');
        }
        return sb.toString();
    }

    private static String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }

    private static String line(int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append('+');
        }
        return sb.toString();
    }
}
