package dev.novov.duckdb.bench.api;

public sealed interface QueryCase permits AvgByDistrictCase, DescribeCase, GroupByYearCase,
        HeadCase, MedianByDistrictCase, NewBuildVsOldCase, FilterCase, GroupByCase
        , TopKCase {
    String id();
}
