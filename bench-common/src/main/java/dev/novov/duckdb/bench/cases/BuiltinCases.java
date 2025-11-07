package dev.novov.duckdb.bench.cases;

import dev.novov.duckdb.bench.api.AggFn;
import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.GroupByCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.TopKCase;

import java.util.List;
import java.util.Objects;

public final class BuiltinCases {
    private BuiltinCases() {
    }

    public static List<QueryCase> chicagoTaxiCases(String parquetPath) {
        return chicagoTaxiCases(parquetPath, TaxiColumns.defaults());
    }

    public static List<QueryCase> chicagoTaxiCases(String parquetPath, TaxiColumns columns) {
        Objects.requireNonNull(columns, "columns");
        String filterExpr = columns.timestampColumn() + " BETWEEN '2019-01-01' AND '2019-01-31'";
        return chicagoTaxiCases(parquetPath, columns, filterExpr);
    }

    public static List<QueryCase> chicagoTaxiCases(String parquetPath, TaxiColumns columns, String filterExpr) {
        Objects.requireNonNull(parquetPath, "parquetPath");
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(filterExpr, "filterExpr");
        return List.of(
                new GroupByCase(
                        "taxi_group_by_passenger",
                        parquetPath,
                        columns.passengerCountColumn(),
                        columns.aggregateColumn(),
                        AggFn.AVG
                ),
                new FilterCase(
                        "taxi_filter_ts",
                        parquetPath,
                        filterExpr
                ),
                new TopKCase(
                        "taxi_topk_fare",
                        parquetPath,
                        columns.fareColumn(),
                        20,
                        true
                )
        );
    }

    public record TaxiColumns(
            String passengerCountColumn,
            String aggregateColumn,
            String timestampColumn,
            String fareColumn
    ) {
        public static TaxiColumns defaults() {
            return new TaxiColumns("passenger_count", "trip_total", "pickup_ts", "trip_total");
        }
    }
}
