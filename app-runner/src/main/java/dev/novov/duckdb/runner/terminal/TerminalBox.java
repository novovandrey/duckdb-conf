package dev.novov.duckdb.runner.terminal;

import java.util.ArrayList;
import java.util.List;

public final class TerminalBox {
    private TerminalBox() {
    }

    public static String render(String title, List<String> rows, int width) {
        int w = Math.max(60, width);
        StringBuilder sb = new StringBuilder();
        String border = "-".repeat(Math.max(1, w - title.length() - 8));
        sb.append("╭─ ").append(title).append(' ').append(border).append("╮").append(System.lineSeparator());
        for (String row : rows) {
            String text = " " + row;
            if (text.length() > w - 2) {
                text = text.substring(0, w - 2);
            }
            sb.append("│").append(String.format("%-" + (w - 2) + "s", text)).append("│").append(System.lineSeparator());
        }
        sb.append("╰").append("─".repeat(w - 2)).append("╯");
        return sb.toString();
    }

    public static List<String> keyValues(List<TerminalLine> lines) {
        int max = lines.stream().mapToInt(l -> l.key().length()).max().orElse(4);
        List<String> out = new ArrayList<>();
        for (TerminalLine line : lines) {
            out.add(String.format("%-" + max + "s  %s", line.key(), line.value()));
        }
        return out;
    }
}
