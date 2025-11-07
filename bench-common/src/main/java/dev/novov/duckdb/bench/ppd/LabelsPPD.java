package dev.novov.duckdb.bench.ppd;

/** SQL snippets that map single-letter codes to readable labels. */
public final class LabelsPPD {
    private LabelsPPD() {
    }

    /** CASE expression yielding property type names. */
    public static final String PROPERTY_TYPE_NAME = """
            CASE property_type
              WHEN 'D' THEN 'Detached'
              WHEN 'S' THEN 'Semi-Detached'
              WHEN 'T' THEN 'Terraced'
              WHEN 'F' THEN 'Flat/Maisonette'
              ELSE 'Other'
            END
            """;

    public static final String IS_NEW_BUILD = "(new_build = 'Y')";

    public static final String TENURE = """
            CASE duration
              WHEN 'F' THEN 'Freehold'
              WHEN 'L' THEN 'Leasehold'
              ELSE 'Unknown'
            END
            """;

    /** Compact projection for "head" demos: readable fields first. */
    public static String headProjection() {
        return """
                %s AS property_type_name,
                %s AS is_new_build,
                %s AS tenure,
                price, transfer_date, postcode, district, county, town_city
                """.formatted(PROPERTY_TYPE_NAME, IS_NEW_BUILD, TENURE);
    }
}
