package dev.novov.duckdb.bench.cases;

import dev.novov.duckdb.bench.api.AvgByDistrictCase;
import dev.novov.duckdb.bench.api.DescribeCase;
import dev.novov.duckdb.bench.api.GroupByYearCase;
import dev.novov.duckdb.bench.api.HeadCase;
import dev.novov.duckdb.bench.api.MedianByDistrictCase;
import dev.novov.duckdb.bench.api.NewBuildVsOldCase;
import dev.novov.duckdb.bench.api.QueryCase;

import java.util.List;

public final class BuiltinCasesPPD {
    private BuiltinCasesPPD() {
    }

    public static List<QueryCase> all(String file) {
        return List.of(
                new DescribeCase("ppd_describe", file),
                new HeadCase("ppd_head_5", file, 5),
                new GroupByYearCase("ppd_sales_by_year", file),
                new AvgByDistrictCase("ppd_avg_by_district", file, 1_000),
                new NewBuildVsOldCase("ppd_new_vs_old", file),
                new MedianByDistrictCase("ppd_median_by_district", file, 1_000, 20)
        );
    }
}
