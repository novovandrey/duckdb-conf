package dev.novov.duckdb.engines;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.AsciiTable;
import dev.novov.duckdb.bench.util.GC;
import dev.novov.duckdb.bench.util.MemoryUtil;
import dev.novov.duckdb.bench.util.Paths2;
import dev.novov.duckdb.bench.util.Stopwatch;
import dev.novov.duckdb.engines.duck.DuckSqlPPD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class DuckdbEngine implements AnalyticsEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(DuckdbEngine.class);
    private static final Duration GC_PAUSE = Duration.ofMillis(200);

    @Override
    public String name() {
        return "duckdb";
    }

    @Override
    public CaseResult run(QueryCase queryCase, RunConfig config) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            configureConnection(connection, config);
            String sql = DuckdbSql.build(queryCase, config);
            LOGGER.info("[duckdb] Case {} SQL:{}{}", queryCase.id(), System.lineSeparator(), sql);

            for (int i = 0; i < config.warmupRuns(); i++) {
                executeOnce(connection, sql, config.explain(), queryCase.id());
            }

            List<CaseRun> runs = new ArrayList<>(config.measuredRuns());
            for (int i = 0; i < config.measuredRuns(); i++) {
                if (config.coldJVM()) {
                    GC.requestGc(GC_PAUSE);
                }
                runs.add(executeOnce(connection, sql, config.explain(), queryCase.id()));
            }

            return new CaseResult(queryCase, name(), runs);
        }
    }

    public void runInteractive(QueryCase queryCase, RunConfig config) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            configureConnection(connection, config);
            String sql = DuckdbSql.build(queryCase, config);
            LOGGER.info("[duckdb] Running interactive query {} SQL:{}{}", queryCase.id(), System.lineSeparator(), sql);
            try (Statement statement = connection.createStatement()) {
                boolean hasResult = statement.execute(sql);
                while (hasResult) {
                    try (ResultSet rs = statement.getResultSet()) {
                        printResultSet(rs);
                    }
                    hasResult = statement.getMoreResults();
                }
            }
        }
    }

    public void csvToParquet(String csvFileOrUrl, String outParquet, int threads) throws SQLException {
        RunConfig config = new RunConfig(0, 1, threads, -1, false, false);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            configureConnection(connection, config);
            csvToParquet(connection, csvFileOrUrl, outParquet);
        }
    }

    public void csvToParquet(Connection conn, String csvFileOrUrl, String outParquet) throws SQLException {
        String in = Paths2.normalizePathOrUrl(csvFileOrUrl);
        String out = Paths2.normalizePathOrUrl(outParquet);
        String sql = "COPY (SELECT * FROM " + DuckSqlPPD.fromCsv(in) + ") "
                + "TO '" + escape(out) + "' (FORMAT PARQUET)";
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String escape(String raw) {
        return raw.replace("'", "''");
    }

    private void configureConnection(Connection connection, RunConfig config) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSTALL httpfs;");
            statement.execute("LOAD httpfs;");
            if (config.threads() > 0) {
                statement.execute("PRAGMA threads=" + config.threads());
            }
            if (config.explain()) {
                statement.execute("PRAGMA enable_profiling = 'json'");
                statement.execute("PRAGMA profiling_output = 'duck_profile.json'");
            }
        }
    }

    private CaseRun executeOnce(Connection connection, String sql, boolean capturePlan, String caseId) throws SQLException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long before = MemoryUtil.sampleUsedBytes();
        long rows = 0L;
        StringBuilder explainOutput = capturePlan ? new StringBuilder() : null;

        try (Statement statement = connection.createStatement()) {
            boolean hasResult = statement.execute(sql);
            while (hasResult) {
                try (ResultSet rs = statement.getResultSet()) {
                    rows += consumeResultSet(rs, explainOutput);
                }
                hasResult = statement.getMoreResults();
            }
        }

        stopwatch.stop();
        long after = MemoryUtil.sampleUsedBytes();
        if (explainOutput != null && explainOutput.length() > 0) {
            LOGGER.info("[duckdb] EXPLAIN ANALYZE for {}:{}{}", caseId, System.lineSeparator(), explainOutput);
        }
        long deltaMem2 = MemoryUtil.sampleUsedBytesV2();
        return new CaseRun(stopwatch.elapsedNanos(), rows, -1L, deltaMem2);
    }

    private static long consumeResultSet(ResultSet rs, StringBuilder explainOutput) throws SQLException {
        long rows = 0L;
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();
        while (rs.next()) {
            rows++;
            if (explainOutput != null) {
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        explainOutput.append('\t');
                    }
                    explainOutput.append(rs.getString(i));
                }
                explainOutput.append(System.lineSeparator());
            }
        }
        return rows;
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();
        if (columns == 0) {
            return;
        }
        List<String> headers = new ArrayList<>(columns);
        for (int i = 1; i <= columns; i++) {
            headers.add(metaData.getColumnLabel(i));
        }
        AsciiTable table = new AsciiTable(headers);
        while (rs.next()) {
            Object[] row = new Object[columns];
            for (int i = 1; i <= columns; i++) {
                row[i - 1] = rs.getString(i);
            }
            table.addRow(row);
        }
        System.out.println(table.render());
    }
}
