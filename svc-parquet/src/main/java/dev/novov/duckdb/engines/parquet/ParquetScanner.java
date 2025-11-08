package dev.novov.duckdb.engines.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.hadoop.api.ReadSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class ParquetScanner implements AutoCloseable {
    private static final Map<String, List<String>> ALIASES = Map.ofEntries(
            Map.entry("trip_total", List.of("trip_total", "total_amount")),
            Map.entry("total_amount", List.of("total_amount", "trip_total")),
            Map.entry("passenger_count", List.of("passenger_count", "passenger_cnt")),
            Map.entry("passenger_cnt", List.of("passenger_cnt", "passenger_count")),
            Map.entry("pickup_ts", List.of("pickup_ts", "pickup_datetime", "trip_start_timestamp")),
            Map.entry("pickup_datetime", List.of("pickup_datetime", "pickup_ts", "trip_start_timestamp")),
            Map.entry("fare", List.of("fare", "fare_amount", "trip_total")),
            Map.entry("fare_amount", List.of("fare_amount", "fare", "trip_total"))
    );

    private final ParquetReader<Group> reader;
    private final Map<String, ColumnMetadata> metadata;

    private ParquetScanner(ParquetReader<Group> reader, Map<String, ColumnMetadata> metadata) {
        this.reader = reader;
        this.metadata = metadata;
    }

    static ParquetScanner open(String file, List<String> requestedColumns) throws IOException {
        Objects.requireNonNull(file, "file");
        if (requestedColumns == null || requestedColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be requested");
        }
        Path path = new Path(file);
        Configuration conf = new Configuration();
        conf.setBoolean("parquet.filter.statistics.enabled", true);
        conf.setBoolean("parquet.filter.dictionary.enabled", true);
        conf.setBoolean("parquet.filter.columnindex.enabled", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        Projection projection = prepareProjection(path, conf, requestedColumns);
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, projection.projectedSchema().toString());
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();
        return new ParquetScanner(reader, projection.metadata());
    }

    private static Projection prepareProjection(Path path, Configuration conf, List<String> requestedColumns) throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
            Map<String, ColumnMetadata> metadata = new HashMap<>();
            List<Type> projectedTypes = new ArrayList<>();
            Map<String, String> logicalToPhysical = new LinkedHashMap<>();
            for (String logical : requestedColumns) {
                String physical = resolvePhysicalColumn(schema, logical);
                if (logicalToPhysical.containsKey(logical)) {
                    continue;
                }
                Type type = schema.getType(physical);
                projectedTypes.add(type);
                logicalToPhysical.put(logical, type.getName());
            }
            MessageType projection = new MessageType(schema.getName(), projectedTypes);
            for (Map.Entry<String, String> entry : logicalToPhysical.entrySet()) {
                String logical = entry.getKey();
                String physical = entry.getValue();
                int index = projection.getFieldIndex(physical);
                Type type = projection.getType(physical);
                if (!type.isPrimitive()) {
                    throw new IllegalArgumentException("Only primitive fields are supported: " + physical);
                }
                PrimitiveType.PrimitiveTypeName primitive = type.asPrimitiveType().getPrimitiveTypeName();
                metadata.put(logical, new ColumnMetadata(logical, physical, index, primitive));
            }
            return new Projection(schema, projection, metadata);
        }
    }

    private static String resolvePhysicalColumn(MessageType schema, String logical) {
        String normalized = logical.toLowerCase(Locale.ROOT);
        List<String> candidates = new ArrayList<>();
        candidates.add(logical);
        candidates.addAll(ALIASES.getOrDefault(normalized, List.of()));
        for (String candidate : candidates) {
            String physical = findColumn(schema, candidate);
            if (physical != null) {
                return physical;
            }
        }
        throw new IllegalArgumentException("Column '" + logical + "' not found in schema " + schema);
    }

    private static String findColumn(MessageType schema, String candidate) {
        for (Type type : schema.getFields()) {
            if (type.getName().equalsIgnoreCase(candidate)) {
                return type.getName();
            }
        }
        return null;
    }

    public void scan(GroupConsumer consumer, long rowLimit) throws IOException {
        Group group;
        long processed = 0L;
        while ((group = reader.read()) != null) {
            if (rowLimit > 0 && processed >= rowLimit) {
                break;
            }
            processed++;
            consumer.accept(group);
        }
    }

    public String getKey(Group group, String logicalColumn) {
        ColumnMetadata meta = metadata(logicalColumn);
        if (!hasValue(group, meta)) {
            return "NULL";
        }
        return group.getValueToString(meta.fieldIndex(), 0);
    }

    public String getString(Group group, String logicalColumn) {
        ColumnMetadata meta = metadata(logicalColumn);
        if (!hasValue(group, meta)) {
            return null;
        }
        return switch (meta.typeName()) {
            case BINARY -> group.getBinary(meta.fieldIndex(), 0).toStringUsingUTF8();
            default -> group.getValueToString(meta.fieldIndex(), 0);
        };
    }

    public double getDouble(Group group, String logicalColumn) {
        ColumnMetadata meta = metadata(logicalColumn);
        if (!hasValue(group, meta)) {
            return Double.NaN;
        }
        return switch (meta.typeName()) {
            case DOUBLE -> group.getDouble(meta.fieldIndex(), 0);
            case FLOAT -> group.getFloat(meta.fieldIndex(), 0);
            case INT64 -> group.getLong(meta.fieldIndex(), 0);
            case INT32 -> group.getInteger(meta.fieldIndex(), 0);
            case BOOLEAN -> group.getBoolean(meta.fieldIndex(), 0) ? 1d : 0d;
            case BINARY -> Double.parseDouble(group.getBinary(meta.fieldIndex(), 0).toStringUsingUTF8());
            case INT96 -> Double.parseDouble(group.getValueToString(meta.fieldIndex(), 0));
            default -> Double.parseDouble(group.getValueToString(meta.fieldIndex(), 0));
        };
    }

    private ColumnMetadata metadata(String logicalColumn) {
        ColumnMetadata meta = metadata.get(logicalColumn);
        if (meta == null) {
            throw new IllegalArgumentException("Unknown column mapping for " + logicalColumn);
        }
        return meta;
    }

    private static boolean hasValue(Group group, ColumnMetadata metadata) {
        return group.getFieldRepetitionCount(metadata.fieldIndex()) > 0;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @FunctionalInterface
    interface GroupConsumer {
        void accept(Group group) throws IOException;
    }

    private record ColumnMetadata(String logicalName,
                                  String physicalName,
                                  int fieldIndex,
                                  PrimitiveType.PrimitiveTypeName typeName) {
    }

    private record Projection(MessageType fileSchema,
                              MessageType projectedSchema,
                              Map<String, ColumnMetadata> metadata) {
    }
}
