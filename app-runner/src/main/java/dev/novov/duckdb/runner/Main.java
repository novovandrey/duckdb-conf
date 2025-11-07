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
        return new RunnerOptions(engine, caseFilter, file, resolvedThreads, warmups, runs, explain, limitRows);
    }
}
