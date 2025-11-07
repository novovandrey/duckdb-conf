package dev.novov.duckdb.runner;

import dev.novov.duckdb.bench.api.QueryCase;
import dev.novov.duckdb.bench.cases.BuiltinCases;
import dev.novov.duckdb.bench.cases.BuiltinCasesPPD;
import dev.novov.duckdb.bench.util.Files2;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class CaseSelector {
    List<QueryCase> select(DatasetOption dataset, String filter, String file) {
        validateFile(file);
        List<QueryCase> cases = dataset == DatasetOption.PPD
                ? BuiltinCasesPPD.all(file)
                : BuiltinCases.chicagoTaxiCases(file);
        if (filter == null || filter.equalsIgnoreCase("all")) {
            return cases;
        }
        Set<String> wanted = new LinkedHashSet<>();
        Arrays.stream(filter.split(","))
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .map(it -> it.toLowerCase(Locale.ROOT))
                .forEach(wanted::add);
        if (wanted.isEmpty()) {
            throw new IllegalArgumentException("No valid case ids specified");
        }
        List<QueryCase> selected = new ArrayList<>();
        for (QueryCase queryCase : cases) {
            if (wanted.remove(queryCase.id().toLowerCase(Locale.ROOT))) {
                selected.add(queryCase);
            }
        }
        if (!wanted.isEmpty()) {
            throw new IllegalArgumentException("Unknown case ids: " + wanted);
        }
        return selected;
    }

    private void validateFile(String file) {
        if (file.startsWith("http://") || file.startsWith("https://")) {
            if (!Files2.exists(file)) {
                throw new IllegalArgumentException("Remote file appears unreachable: " + file);
            }
            System.out.println("Using remote file: " + file);
            return;
        }
        Path resolved = Files2.requireLocalFile(file);
        System.out.println("Using local file: " + resolved);
    }
}
