package io.epiphanous.flink.formats.avro.registry.glue.debezium;

import java.util.Map;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.*;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatFactory;

public class DebeziumAvroGlueFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AvroGlueFormatFactory.class);

  public static final String IDENTIFIER = "debezium-avro-glue";

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context,
      ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    String schemaName = formatOptions.get(SCHEMA_NAME);

    // AvroGlueFormatFactory.buildConfigs changed from private to public static
    Map<String, Object> configs = AvroGlueFormatFactory.buildConfigs(formatOptions);

    // TODO: Add logging?
    if (LOG.isDebugEnabled()) {
      LOG.debug("createDecodingFormat() with schemaName {} and configs {}", schemaName, configs);
    }

    return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context,
          DataType producedDataType,
          int[][] projections) {
        producedDataType = Projection.of(projections).project(producedDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);
        return new DebeziumAvroGlueDeserializationSchema(
          rowType,
          producedTypeInfo,
          schemaRegistryURL,
          schema,
          optionalPropertiesMap
        );
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_BEFORE)
            .addContainedKind(RowKind.UPDATE_AFTER)
            .addContainedKind(RowKind.DELETE)
            .build();
      }
    };
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {

    FactoryUtil.validateFactoryOptions(this, formatOptions);
    String schemaRegistryURL = formatOptions.get(URL);
    Optional<String> subject = formatOptions.getOptional(SUBJECT);
    String schema = formatOptions.getOptional(SCHEMA).orElse(null);
    Map<String, ?> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

    if (!subject.isPresent()) {
      throw new ValidationException(
          String.format(
              "Option '%s.%s' is required for serialization",
              IDENTIFIER, SUBJECT.key()));
    }

    return new EncodingFormat<SerializationSchema<RowData>>() {
      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_BEFORE)
            .addContainedKind(RowKind.UPDATE_AFTER)
            .addContainedKind(RowKind.DELETE)
            .build();
      }

      @Override
      public SerializationSchema<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new DebeziumAvroSerializationSchema(
            rowType, schemaRegistryURL, subject.get(), schema, optionalPropertiesMap);
      }
    };
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(URL);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(SUBJECT);
    options.add(PROPERTIES);
    options.add(SCHEMA);
    options.add(SSL_KEYSTORE_LOCATION);
    options.add(SSL_KEYSTORE_PASSWORD);
    options.add(SSL_TRUSTSTORE_LOCATION);
    options.add(SSL_TRUSTSTORE_PASSWORD);
    options.add(BASIC_AUTH_CREDENTIALS_SOURCE);
    options.add(BASIC_AUTH_USER_INFO);
    options.add(BEARER_AUTH_CREDENTIALS_SOURCE);
    options.add(BEARER_AUTH_TOKEN);
    return options;
  }

  static void validateSchemaString(@Nullable String schemaString, RowType rowType) {
    if (schemaString != null) {
      LogicalType convertedDataType = AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();

      if (!convertedDataType.equals(rowType)) {
        throw new IllegalArgumentException(
            format(
                "Schema provided for '%s' format must be a nullable record type with fields 'before', 'after', 'op'"
                    + " and schema of fields 'before' and 'after' must match the table schema: %s",
                IDENTIFIER, schemaString));
      }
    }
  }
}
