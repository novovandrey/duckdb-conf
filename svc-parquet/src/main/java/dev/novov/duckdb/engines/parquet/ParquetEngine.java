package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.bench.api.CaseResult;
import dev.novov.duckdb.bench.api.CaseRun;
import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.GroupByCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.TopKCase;
import dev.novov.duckdb.bench.util.GC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Manual Parquet reader that implements the same benchmark SPI without delegating to a database.
 */
public final class ParquetEngine implements AnalyticsEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetEngine.class);
    private static final Duration GC_PAUSE = Duration.ofMillis(200);

    private final GroupByExecutor groupByExecutor = new GroupByExecutor();
    private final FilterExecutor filterExecutor = new FilterExecutor();
    private final TopKExecutor topKExecutor = new TopKExecutor();

    @Override
    public String name() {
        return "parquet-manual";
    }

    @Override
    public CaseResult run(QueryCase queryCase, RunConfig config) throws Exception {
        LOGGER.info("[parquet] Running {} with {}", queryCase.id(), config);
        for (int i = 0; i < config.warmupRuns(); i++) {
            execute(queryCase, config);
        }
        List<CaseRun> runs = new ArrayList<>(config.measuredRuns());
        for (int i = 0; i < config.measuredRuns(); i++) {
            if (config.coldJVM()) {
                GC.requestGc(GC_PAUSE);
            }
            runs.add(execute(queryCase, config));
        }
        return new CaseResult(queryCase, name(), runs);
    }

    private CaseRun execute(QueryCase queryCase, RunConfig config) throws Exception {
        return switch (queryCase) {
            case GroupByCase groupBy -> groupByExecutor.execute(groupBy, config);
            case FilterCase filter -> filterExecutor.execute(filter, config);
            case TopKCase topK -> topKExecutor.execute(topK, config);
        };
    }
}
