package dev.novov.duckdb.engines.parquet;

import dev.novov.duckdb.bench.api.*;
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
    private final PpdSalesByYearExecutor ppdSalesByYear = new PpdSalesByYearExecutor();

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
            case GroupByYearCase c -> ppdSalesByYear.execute(c, config);
//            case PpdAvgByDistrictCase c -> ppdAvgByDistrict.execute(c, config);
//            case PpdNewVsOldCase c -> ppdNewVsOld.execute(c, config);
//            case PpdMedianByDistrictCase c -> ppdMedianByDistrict.execute(c, config);
            case DescribeCase c -> throw new UnsupportedOperationException("Describe is DuckDB-only");
            case HeadCase c -> throw new UnsupportedOperationException("Head is DuckDB-only");
            default -> throw new IllegalStateException("Unexpected value: " + queryCase);
        };
    }
}
