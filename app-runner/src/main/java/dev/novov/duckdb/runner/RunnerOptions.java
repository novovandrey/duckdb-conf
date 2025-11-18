package dev.novov.duckdb.runner;

record RunnerOptions(
        EngineOption engineOption,
        DatasetOption datasetOption,
        String caseFilter,
        String file,
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
