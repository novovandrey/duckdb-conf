package dev.novov.duckdb.runner;

import java.util.Locale;

enum EngineOption {
    DUCKDB("duckdb"),
    PARQUET("parquet"),
    BOTH("both");

    private final String cliName;

    EngineOption(String cliName) {
        this.cliName = cliName;
    }

    static EngineOption from(String value) {
        if (value == null) {
            return BOTH;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (EngineOption option : values()) {
            if (option.cliName.equals(normalized)) {
                return option;
            }
        }
        throw new IllegalArgumentException("Unknown engine option: " + value);
    }

    @Override
    public String toString() {
        return cliName;
    }

    boolean includesDuckdb() {
        return this == DUCKDB || this == BOTH;
    }

    boolean includesParquet() {
        return this == PARQUET || this == BOTH;
    }
}
