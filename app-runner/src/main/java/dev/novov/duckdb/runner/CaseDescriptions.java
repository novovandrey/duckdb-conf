package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.GroupByCase;
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
        };
    }
}
