package dev.novov.duckdb.bench.ppd;

public final class ColumnsPPD {
    private ColumnsPPD() {
    }

    /**
     * Explicit schema for headerless pp-complete.csv.
     */
    public static final String COLUMNS_SQL_MAP = """
            columns={
              'transaction_id':'VARCHAR',
              'price':'BIGINT',
              'transfer_date':'DATE',
              'postcode':'VARCHAR',
              'property_type':'VARCHAR',
              'new_build':'VARCHAR',
              'duration':'VARCHAR',
              'paon':'VARCHAR',
              'saon':'VARCHAR',
              'street':'VARCHAR',
              'locality':'VARCHAR',
              'town_city':'VARCHAR',
              'district':'VARCHAR',
              'county':'VARCHAR',
              'ppd_category':'VARCHAR',
              'record_status':'VARCHAR'
            }
            """;
}
