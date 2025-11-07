package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.AvgByDistrictCase;
import dev.novov.duckdb.bench.api.DescribeCase;
import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.GroupByCase;
import dev.novov.duckdb.bench.api.GroupByYearCase;
import dev.novov.duckdb.bench.api.HeadCase;
import dev.novov.duckdb.bench.api.MedianByDistrictCase;
import dev.novov.duckdb.bench.api.NewBuildVsOldCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.TopKCase;

final class CaseDescriptions {
    private CaseDescriptions() {
    }

    static String describe(QueryCase queryCase) {
        return switch (queryCase) {
            case GroupByCase groupBy -> "group=" + groupBy.groupCol() + ", agg=" + groupBy.aggFn()
                    + "(" + groupBy.aggCol() + ")";
            case FilterCase filter -> "filter=" + filter.filterExpr();
            case TopKCase topK -> "orderBy=" + topK.orderByCol() + ", k=" + topK.k()
                    + ", desc=" + topK.desc();
            case DescribeCase describe -> "describe " + describe.file();
            case HeadCase head -> "head limit=" + head.limit();
            case GroupByYearCase ignored -> "sales by year (ppd_category='A')";
            case AvgByDistrictCase avg -> "avg price by district, minCount=" + avg.minCount();
            case NewBuildVsOldCase ignored -> "new build vs old (ppd_category='A')";
            case MedianByDistrictCase median -> "median price by district, minCount=" + median.minCount()
                    + ", limit=" + median.limit();
        };
    }
}
