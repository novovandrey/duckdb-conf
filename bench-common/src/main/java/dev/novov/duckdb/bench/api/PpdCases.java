package dev.novov.duckdb.bench.api;

public record DescribeCase(String id, String file) implements QueryCase {
}

public record HeadCase(String id, String file, int limit) implements QueryCase {
}

public record GroupByYearCase(String id, String file) implements QueryCase {
}

public record AvgByDistrictCase(String id, String file, int minCount) implements QueryCase {
}

public record NewBuildVsOldCase(String id, String file) implements QueryCase {
}

public record MedianByDistrictCase(String id, String file, int minCount, int limit) implements QueryCase {
}
