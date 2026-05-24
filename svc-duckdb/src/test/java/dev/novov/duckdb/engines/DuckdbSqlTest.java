package dev.novov.duckdb.engines;

import dev.novov.duckdb.bench.api.CrossSourceJoinCase;
import dev.novov.duckdb.bench.api.RunConfig;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckdbSqlTest {
    @Test
    void crossSourceJoinUsesValidFromClauseForCsv() {
        CrossSourceJoinCase queryCase = new CrossSourceJoinCase(
                "cross_source_join",
                "C:/data/pp-complete.parquet",
                "C:/data/postcode_regions.csv"
        );
        RunConfig config = new RunConfig(0, 1, 8, -1, false, false, false);

        String sql = DuckdbSql.build(queryCase, config);

        assertTrue(Pattern.compile("FROM\\s+read_csv_auto\\('C:/data/postcode_regions\\.csv'\\)")
                .matcher(sql)
                .find());
        assertFalse(sql.contains("FROMread_csv_auto("));
    }
}
