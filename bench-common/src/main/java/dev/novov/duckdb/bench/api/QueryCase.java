package dev.novov.duckdb.bench.api;

public sealed interface QueryCase permits AvgByDistrictCase, DescribeCase, GroupByYearCase,
        HeadCase, MedianByDistrictCase, NewBuildVsOldCase, FilterCase, GroupByCase, CrossSourceJoinCase
        , TopKCase {
    String id();
}
