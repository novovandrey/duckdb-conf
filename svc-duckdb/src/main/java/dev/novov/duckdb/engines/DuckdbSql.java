package dev.novov.duckdb.engines;

import dev.novov.duckdb.bench.api.AggFn;
import dev.novov.duckdb.bench.api.FilterCase;
import dev.novov.duckdb.bench.api.GroupByCase;
import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.api.RunConfig;
import dev.novov.duckdb.bench.api.TopKCase;

final class DuckdbSql {
    private DuckdbSql() {
    }

    static String build(QueryCase queryCase, RunConfig config) {
        String core = switch (queryCase) {
            case GroupByCase groupByCase -> groupBy(groupByCase);
            case FilterCase filterCase -> filter(filterCase);
            case TopKCase topKCase -> topK(topKCase);
        };
        if (config.limitRowsOrMinusOne() > 0) {
            core = "SELECT * FROM (" + core + ") AS duck_query LIMIT " + config.limitRowsOrMinusOne();
        }
        if (config.explain()) {
            core = "EXPLAIN ANALYZE " + core;
        }
        return core;
    }

    static String caseFile(QueryCase queryCase) {
        return switch (queryCase) {
            case GroupByCase groupByCase -> groupByCase.file();
            case FilterCase filterCase -> filterCase.file();
            case TopKCase topKCase -> topKCase.file();
        };
    }

    static boolean isRemoteFile(String file) {
        return file.startsWith("http://") || file.startsWith("https://");
    }

    private static String groupBy(GroupByCase groupBy) {
        String agg = switch (groupBy.aggFn()) {
            case COUNT -> "COUNT";
            case AVG -> "AVG";
            case SUM -> "SUM";
        };
        return "SELECT " + ident(groupBy.groupCol()) + ", " + agg + "(" + ident(groupBy.aggCol()) + ") AS metric "
                + "FROM " + fileLiteral(groupBy.file()) + " GROUP BY 1";
    }

    private static String filter(FilterCase filter) {
        return "SELECT COUNT(*) AS cnt FROM " + fileLiteral(filter.file())
                + " WHERE " + filter.filterExpr();
    }

    private static String topK(TopKCase topK) {
        return "SELECT " + ident(topK.orderByCol()) + " FROM " + fileLiteral(topK.file())
                + " ORDER BY " + ident(topK.orderByCol()) + (topK.desc() ? " DESC" : " ASC")
                + " LIMIT " + topK.k();
    }

    private static String ident(String candidate) {
        return "\"" + candidate.replace("\"", "\"\"") + "\"";
    }

    private static String fileLiteral(String file) {
        return "'" + file.replace("'", "''") + "'";
    }
}
