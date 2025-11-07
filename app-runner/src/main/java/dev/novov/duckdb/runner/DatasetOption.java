package dev.novov.duckdb.runner;

import java.util.Locale;

enum DatasetOption {
    TAXI("taxi"),
    PPD("ppd");

    private final String cliName;

    DatasetOption(String cliName) {
        this.cliName = cliName;
    }

    static DatasetOption from(String value) {
        if (value == null || value.isBlank()) {
            return TAXI;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (DatasetOption option : values()) {
            if (option.cliName.equals(normalized)) {
                return option;
            }
        }
        throw new IllegalArgumentException("Unknown dataset: " + value);
    }

    boolean isPpd() {
        return this == PPD;
    }

    @Override
    public String toString() {
        return cliName;
    }
}
