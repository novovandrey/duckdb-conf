package dev.novov.duckdb.engines.duck;

import dev.novov.duckdb.bench.ppd.ColumnsPPD;

public final class DuckSqlPPD {
    private DuckSqlPPD() {
    }

    public static String fromCsv(String fileOrUrl) {
        return "read_csv_auto('" + escape(fileOrUrl) + "', HEADER=false, " + ColumnsPPD.COLUMNS_SQL_MAP + ")";
    }

    private static String escape(String location) {
        return location.replace("'", "''");
    }
}
