package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.AnalyticsEngine;
import dev.novov.duckdb.engines.duckdb.DuckdbEngine;
import dev.novov.duckdb.engines.parquet.ParquetEngine;

import java.util.List;

final class EngineFactory {
    List<AnalyticsEngine> create(EngineOption option) {
        return switch (option) {
            case DUCKDB -> List.of(new DuckdbEngine());
            case PARQUET -> List.of(new ParquetEngine());
            case BOTH -> List.of(new DuckdbEngine(), new ParquetEngine());
        };
    }
}
