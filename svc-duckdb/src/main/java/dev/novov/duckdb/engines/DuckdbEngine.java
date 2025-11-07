package dev.novov.duckdb.engines;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.util.GC;
import dev.novov.duckdb.bench.util.MemoryUtil;
import dev.novov.duckdb.bench.util.Stopwatch;
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
            configureConnection(connection, queryCase, config);
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

    private void configureConnection(Connection connection, QueryCase queryCase, RunConfig config) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int threads = Math.max(1, config.threads());
            statement.execute("PRAGMA threads = " + threads);
            String file = DuckdbSql.caseFile(queryCase);
            if (DuckdbSql.isRemoteFile(file)) {
                statement.execute("INSTALL httpfs");
                statement.execute("LOAD httpfs");
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
        long maxUsed = Math.max(before, after);
        return new CaseRun(stopwatch.elapsedNanos(), rows, -1L, maxUsed);
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
}
