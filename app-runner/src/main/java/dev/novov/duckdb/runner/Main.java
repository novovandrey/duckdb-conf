package dev.novov.duckdb.runner;

import picocli.CommandLine;

@CommandLine.Command(
        name = "duckdb-conf",
        mixinStandardHelpOptions = true,
        description = "Runs DuckDB and manual Parquet engines on benchmark cases"
)
public final class Main implements Runnable {
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = "--engine", description = "Engine to run: duckdb|parquet|both", defaultValue = "both")
    private String engineOption;

    @CommandLine.Option(names = "--dataset", description = "Dataset to run: taxi|ppd", defaultValue = "taxi")
    private String datasetOption;

    @CommandLine.Option(names = "--case", description = "Case id or 'all'", defaultValue = "all")
    private String caseFilter;

    @CommandLine.Option(names = "--file", description = "Input file path or URL", required = true)
    private String file;

    @CommandLine.Option(names = "--threads", description = "Worker threads", defaultValue = "0")
    private int threads;

    @CommandLine.Option(names = "--warmup", description = "Warmup runs", defaultValue = "1")
    private int warmups;

    @CommandLine.Option(names = "--runs", description = "Measured runs", defaultValue = "5")
    private int runs;

    @CommandLine.Option(names = "--explain", description = "Emit DuckDB EXPLAIN ANALYZE output")
    private boolean explain;

    @CommandLine.Option(names = "--limitRows", description = "Limit rows processed (default unlimited)", defaultValue = "-1")
    private long limitRows;

    @CommandLine.Option(names = "--schema", description = "Describe schema (PPD only) and exit")
    private boolean schemaOnly;

    @CommandLine.Option(names = "--head", description = "Print first N rows (PPD only) and exit", defaultValue = "0", paramLabel = "N")
    private int head;

    @CommandLine.Option(names = "--to-parquet", description = "Convert CSV to Parquet and exit", paramLabel = "FILE")
    private String toParquet;

    public static void main(String[] args) {
        int exit = new CommandLine(new Main()).execute(args);
        System.exit(exit);
    }

    @Override
    public void run() {
        try {
            RunnerOptions options = buildOptions();
            new BenchRunner().run(options);
        } catch (Exception ex) {
            throw new CommandLine.ExecutionException(spec.commandLine(), ex.getMessage(), ex);
        }
    }

    private RunnerOptions buildOptions() {
        EngineOption engine = EngineOption.from(engineOption);
        DatasetOption dataset = DatasetOption.from(datasetOption);
        int resolvedThreads = threads <= 0 ? Runtime.getRuntime().availableProcessors() : threads;
        if (resolvedThreads < 1) {
            throw new IllegalArgumentException("threads must be >= 1");
        }
        if (warmups < 0) {
            throw new IllegalArgumentException("warmup must be >= 0");
        }
        if (runs < 1) {
            throw new IllegalArgumentException("runs must be >= 1");
        }
        if (head < 0) {
            throw new IllegalArgumentException("--head must be >= 0");
        }
        if (schemaOnly && head > 0) {
            throw new IllegalArgumentException("--schema cannot be combined with --head");
        }
        String normalizedToParquet = (toParquet == null || toParquet.isBlank()) ? null : toParquet;
        if (normalizedToParquet != null && (schemaOnly || head > 0)) {
            throw new IllegalArgumentException("--to-parquet cannot be combined with --schema or --head");
        }
        if ((schemaOnly || head > 0) && !dataset.isPpd()) {
            throw new IllegalArgumentException("--schema/head are only supported for the PPD dataset");
        }
        return new RunnerOptions(
                engine,
                dataset,
                caseFilter,
                file,
                resolvedThreads,
                warmups,
                runs,
                explain,
                limitRows,
                schemaOnly,
                head,
                normalizedToParquet
        );
    }
}
