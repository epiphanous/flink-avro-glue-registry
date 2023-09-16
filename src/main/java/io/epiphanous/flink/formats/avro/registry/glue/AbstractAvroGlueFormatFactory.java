package io.epiphanous.flink.formats.avro.registry.glue;

import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.AWS_ENDPOINT;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.AWS_REGION;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.KAFKA_TOPIC;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.PROPERTIES;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.REGISTRY_NAME;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.SCHEMA_AUTO_REGISTRATION_SETTING;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.SCHEMA_NAME;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.SCHEMA_NAMING_GENERATION_CLASS;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.SECONDARY_DESERIALIZER;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;

public abstract class AbstractAvroGlueFormatFactory
    implements DeserializationFormatFactory, SerializationFormatFactory {

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

  @Override
  public abstract String factoryIdentifier();

  protected abstract Logger getLogger();

  protected abstract DeserializationSchema<RowData> getDeserializationSchema(
      RowType rowType,
      TypeInformation<RowData> rowDataTypeInfo,
      String schemaName,
      Map<String, Object> configs);

  protected abstract SerializationSchema<RowData> getSerializationSchema(
      RowType rowType, String schemaName, String topic, Map<String, Object> configs);

  protected abstract ChangelogMode changelogMode();

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {

    FactoryUtil.validateFactoryOptions(this, formatOptions);

    String schemaName = formatOptions.get(SCHEMA_NAME);

    Map<String, Object> configs = buildConfigs(formatOptions);

    if (getLogger().isDebugEnabled()) {
      getLogger()
          .debug("createDecodingFormat() with schemaName {} and configs {}", schemaName, configs);
    }

    return new ProjectableDecodingFormat<>() {
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context, DataType producedDataType, int[][] projections) {
        producedDataType = Projection.of(projections).project(producedDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
            context.createTypeInformation(producedDataType);
        return getDeserializationSchema(rowType, rowDataTypeInfo, schemaName, configs);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return changelogMode();
      }
    };
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {

    FactoryUtil.validateFactoryOptions(this, formatOptions);

    String topic =
        context
            .getConfiguration()
            .getOptional(KAFKA_TOPIC)
            .or(() -> formatOptions.getOptional(KAFKA_TOPIC))
            .orElseThrow(
                () ->
                    new ValidationException(
                        String.format(
                            "Kafka topic not found among table %s options",
                            context.getObjectIdentifier().asSummaryString())));

    String schemaName = formatOptions.get(SCHEMA_NAME);

    Map<String, Object> configs = buildConfigs(formatOptions);

    if (getLogger().isDebugEnabled()) {
      getLogger()
          .debug(
              "createEncodingFormat() with topic {}, schemaName {} and configs {}",
              topic,
              schemaName,
              configs);
    }

    return new EncodingFormat<>() {
      @Override
      public SerializationSchema<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return getSerializationSchema(rowType, schemaName, topic, configs);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return changelogMode();
      }
    };
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return createOptions(SCHEMA_NAME);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return createOptions(
        KAFKA_TOPIC,
        PROPERTIES,
        REGISTRY_NAME,
        AWS_REGION,
        AWS_ENDPOINT,
        SCHEMA_AUTO_REGISTRATION_SETTING,
        SCHEMA_NAMING_GENERATION_CLASS,
        SECONDARY_DESERIALIZER);
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return createOptions(
        REGISTRY_NAME,
        PROPERTIES,
        SCHEMA_NAME,
        AWS_REGION,
        AWS_ENDPOINT,
        SCHEMA_AUTO_REGISTRATION_SETTING,
        SCHEMA_NAMING_GENERATION_CLASS,
        SECONDARY_DESERIALIZER);
  }

  private Set<ConfigOption<?>> createOptions(ConfigOption<?>... configOptions) {
    return new HashSet<>(Arrays.asList(configOptions));
  }
}
