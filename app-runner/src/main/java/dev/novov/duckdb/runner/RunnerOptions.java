package dev.novov.duckdb.runner;

public record RunnerOptions(
        EngineOption engineOption,
        DatasetOption datasetOption,
        String caseFilter,
        String file,
        boolean demoMode,
        int threads,
        int warmups,
        int runs,
        boolean explain,
        boolean explainHtml,
        long limitRows,
        boolean schemaOnly,
        int headLimit,
        String toParquet
) {
}
