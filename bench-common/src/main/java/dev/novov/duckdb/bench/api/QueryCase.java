package dev.novov.duckdb.bench.api;

public sealed interface QueryCase permits
        GroupByCase,
        FilterCase,
        TopKCase,
        DescribeCase,
        HeadCase,
        GroupByYearCase,
        AvgByDistrictCase,
        NewBuildVsOldCase,
        MedianByDistrictCase {
    String id();
}
