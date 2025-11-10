package dev.novov.duckdb.engines;

import dev.novov.duckdb.bench.api.*;
import dev.novov.duckdb.bench.util.Paths2;
import dev.novov.duckdb.engines.duck.DuckSqlPPD;

import static dev.novov.duckdb.bench.ppd.LabelsPPD.IS_NEW_BUILD;
import static dev.novov.duckdb.bench.ppd.LabelsPPD.headProjection;

final class DuckdbSql {
    private DuckdbSql() {
    }

    static String build(QueryCase queryCase, RunConfig config) {
        String core = buildCore(queryCase);
        if (config.limitRowsOrMinusOne() > 0) {
            core = "SELECT * FROM (" + core + ") AS duck_query LIMIT " + config.limitRowsOrMinusOne();
        }
        if (config.explain()) {
            core = "EXPLAIN ANALYZE " + core;
        }
        return core;
    }

    private static String buildCore(QueryCase queryCase) {
        if (isPpdCase(queryCase)) {
            return buildPpd(queryCase);
        }
        if (queryCase instanceof GroupByCase groupByCase) {
            return groupBy(groupByCase);
        }
        if (queryCase instanceof FilterCase filterCase) {
            return filter(filterCase);
        }
        if (queryCase instanceof TopKCase topKCase) {
            return topK(topKCase);
        }
        throw new IllegalArgumentException("Unsupported QueryCase: " + queryCase.getClass().getName());
    }

    private static boolean isPpdCase(QueryCase queryCase) {
        return queryCase instanceof DescribeCase
                || queryCase instanceof HeadCase
                || queryCase instanceof GroupByYearCase
                || queryCase instanceof AvgByDistrictCase
                || queryCase instanceof NewBuildVsOldCase
                || queryCase instanceof MedianByDistrictCase;
    }

    private static String buildPpd(QueryCase queryCase) {
        String file = Paths2.normalizePathOrUrl(ppdFile(queryCase));
        String from = " FROM " + DuckSqlPPD.fromParquet(file) + " ";
        if (queryCase instanceof DescribeCase) {
            return "DESCRIBE SELECT * " + from + " LIMIT 0";
        }
        if (queryCase instanceof HeadCase headCase) {
            int limit = Math.max(1, headCase.limit());
            return "SELECT " + headProjection() + from + " LIMIT " + limit;
        }
        if (queryCase instanceof GroupByYearCase) {
            return """
                    SELECT strftime('%Y', transfer_date) AS year,
                           COUNT(*) AS sales,
                           ROUND(AVG(price)) AS avg_price
                    """ + from + """
                    WHERE ppd_category = 'A'
                    GROUP BY year
                    ORDER BY year
                    """;
        }
        if (queryCase instanceof AvgByDistrictCase avgByDistrictCase) {
            return """
                    SELECT district,
                           COUNT(*) AS n,
                           ROUND(AVG(price)) AS avg_price
                    """ + from + """
                    WHERE ppd_category = 'A'
                    GROUP BY district
                    HAVING n > """ + avgByDistrictCase.minCount() + """
                    ORDER BY avg_price DESC
                    """;
        }
        if (queryCase instanceof NewBuildVsOldCase) {
            return """
                    SELECT
                    """ + IS_NEW_BUILD +    """
                    AS is_new_build,
                           COUNT(*) AS n,
                           ROUND(AVG(price)) AS avg_price
                    """ + from + """
                    WHERE ppd_category = 'A'
                    GROUP BY is_new_build
                    ORDER BY n DESC
                    """;
        }
        if (queryCase instanceof MedianByDistrictCase medianByDistrictCase) {
            return """
                    SELECT district,
                           quantile_cont(price, 0.5) AS median_price,
                           COUNT(*) AS n
                    """ + from + """
                    WHERE ppd_category = 'A'
                    GROUP BY district
                    HAVING n >\s""" + medianByDistrictCase.minCount() + """
                    ORDER BY median_price DESC
                    LIMIT\s""" + medianByDistrictCase.limit();
        }
        throw new IllegalArgumentException("Unsupported PPD case: " + queryCase.getClass().getName());
    }

    private static String ppdFile(QueryCase queryCase) {
        if (queryCase instanceof DescribeCase describeCase) {
            return describeCase.file();
        }
        if (queryCase instanceof HeadCase headCase) {
            return headCase.file();
        }
        if (queryCase instanceof GroupByYearCase groupByYearCase) {
            return groupByYearCase.file();
        }
        if (queryCase instanceof AvgByDistrictCase avgByDistrictCase) {
            return avgByDistrictCase.file();
        }
        if (queryCase instanceof NewBuildVsOldCase newBuildVsOldCase) {
            return newBuildVsOldCase.file();
        }
        if (queryCase instanceof MedianByDistrictCase medianByDistrictCase) {
            return medianByDistrictCase.file();
        }
        throw new IllegalArgumentException("Unsupported PPD case: " + queryCase.getClass().getName());
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
