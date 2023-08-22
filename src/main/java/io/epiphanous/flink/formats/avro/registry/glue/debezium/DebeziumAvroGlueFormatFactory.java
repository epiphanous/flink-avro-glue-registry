package io.epiphanous.flink.formats.avro.registry.glue.debezium;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.*;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
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
            schemaName,
            configs);
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

    String topic = context
        .getConfiguration()
        .getOptional(KAFKA_TOPIC)
        .or(() -> formatOptions.getOptional(KAFKA_TOPIC))
        .orElseThrow(
            () -> new ValidationException(
                String.format(
                    "Kafka topic not found among table %s options",
                    context.getObjectIdentifier().asSummaryString())));

    String schemaName = formatOptions.get(SCHEMA_NAME);

    Map<String, Object> configs = AvroGlueFormatFactory.buildConfigs(formatOptions);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "createEncodingFormat() with topic {}, schemaName {} and configs {}",
          topic,
          schemaName,
          configs);
    }

    return new EncodingFormat<SerializationSchema<RowData>>() {
      @Override
      public SerializationSchema<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new DebeziumAvroGlueSerializationSchema(
            rowType,
            schemaName,
            configs);
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
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(SCHEMA_NAME);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(KAFKA_TOPIC); // required for our tests, but not otherwise
    options.add(PROPERTIES);
    options.add(REGISTRY_NAME);
    options.add(AWS_REGION);
    options.add(AWS_ENDPOINT);
    options.add(SCHEMA_AUTO_REGISTRATION_SETTING);
    options.add(SCHEMA_NAMING_GENERATION_CLASS);
    options.add(SECONDARY_DESERIALIZER);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(REGISTRY_NAME);
    options.add(PROPERTIES);
    options.add(SCHEMA_NAME);
    options.add(AWS_REGION);
    options.add(AWS_ENDPOINT);
    options.add(SCHEMA_AUTO_REGISTRATION_SETTING);
    options.add(SCHEMA_NAMING_GENERATION_CLASS);
    options.add(SECONDARY_DESERIALIZER);
    return options;
  }

  @NotNull
  @VisibleForTesting
  public static Map<String, Object> buildConfigs(ReadableConfig formatOptions) {
    HashMap<String, Object> configs = new HashMap<>();
    configs.put(AWS_REGION.key(), formatOptions.get(AWS_REGION));
    configs.put(SCHEMA_NAME.key(), formatOptions.get(SCHEMA_NAME));
    formatOptions.getOptional(PROPERTIES).ifPresent(configs::putAll);
    formatOptions.getOptional(AWS_ENDPOINT).ifPresent(v -> configs.put(AWS_ENDPOINT.key(), v));
    formatOptions.getOptional(REGISTRY_NAME).ifPresent(v -> configs.put(REGISTRY_NAME.key(), v));
    formatOptions
        .getOptional(SCHEMA_AUTO_REGISTRATION_SETTING)
        .ifPresent(v -> configs.put(SCHEMA_AUTO_REGISTRATION_SETTING.key(), v));
    formatOptions
        .getOptional(SCHEMA_NAMING_GENERATION_CLASS)
        .ifPresent(v -> configs.put(SCHEMA_NAMING_GENERATION_CLASS.key(), v));
    formatOptions
        .getOptional(SECONDARY_DESERIALIZER)
        .ifPresent(v -> configs.put(SECONDARY_DESERIALIZER.key(), v));

    return configs;
  }
}
