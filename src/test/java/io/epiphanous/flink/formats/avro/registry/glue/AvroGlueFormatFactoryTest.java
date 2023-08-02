package io.epiphanous.flink.formats.avro.registry.glue;

import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.AWS_REGION;
import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import javax.validation.constraints.NotNull;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.formats.avro.*;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/** Tests for the {@link AvroGlueFormatFactory}. */
class AvroGlueFormatFactoryTest {

//  final static Logger logger = LoggerFactory.getLogger(AvroGlueFormatFactoryTest.class);

  private static final ResolvedSchema SCHEMA =
      ResolvedSchema.of(
          Column.physical("a", DataTypes.STRING()),
          Column.physical("b", DataTypes.INT()),
          Column.physical("c", DataTypes.BOOLEAN()));
  private static final RowType ROW_TYPE = (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

  private static final String SCHEMA_STRING =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"namespace\": \"my.avro\",\n"
          + "  \"name\": \"test_record\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"a\",\n"
          + "      \"type\": [\n"
          + "        \"null\",\n"
          + "        \"string\"\n"
          + "      ],\n"
          + "      \"default\": null\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"b\",\n"
          + "      \"type\": [\n"
          + "        \"null\",\n"
          + "        \"int\"\n"
          + "      ],\n"
          + "      \"default\": null\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"c\",\n"
          + "      \"type\": [\n"
          + "        \"null\",\n"
          + "        \"boolean\"\n"
          + "      ],\n"
          + "      \"default\": null\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String MY_SCHEMA_NAME = AVRO_SCHEMA.getFullName();
  
  private static final Map<String, String> EXPECTED_OPTIONAL_PROPERTIES = new HashMap<>();

  @Test
  void testDeserializationSchema() {

    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_NAME.key(), MY_SCHEMA_NAME);
    configs.put(AWS_REGION.key(), AWS_REGION.defaultValue());

    final AvroRowDataDeserializationSchema expectedDeser = getDeserializer(configs);
    final DynamicTableSource actualSource =
        FactoryMocks.createTableSource(SCHEMA, getDefaultOptions());

    assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

    TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
        (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
    DeserializationSchema<RowData> actualDeser =
        scanSourceMock.valueFormat.createRuntimeDecoder(
            ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

    assertThat(actualDeser).isEqualTo(expectedDeser);
  }

  @Test
  void testSerializationSchema() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_NAME.key(), MY_SCHEMA_NAME);
    configs.put(AWS_REGION.key(), AWS_REGION.defaultValue());

    final AvroRowDataSerializationSchema expectedSer = getSerializer( "test-topic", configs);

    final DynamicTableSink actualSink = FactoryMocks.createTableSink(SCHEMA, getDefaultOptions());

    assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);

    TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
        (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

    SerializationSchema<RowData> actualSer =
        sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

    assertThat(actualSer).isEqualTo(expectedSer);
  }

  @NotNull
  private Map<String, String> getDefaultOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", TestDynamicTableFactory.IDENTIFIER);
    options.put("target", "MyTarget");
    options.put("buffer-size", "1000");
    options.put("format", AvroGlueFormatFactory.IDENTIFIER);
    options.put("avro-glue.topic", "test-topic");
    options.put("avro-glue.schema.name", MY_SCHEMA_NAME);
    return options;
  }


  @Test
  void factoryIdentifier() {
    assert (Objects.equals(getFactory().factoryIdentifier(), AvroGlueFormatFactory.IDENTIFIER));
  }

  @Test
  void requiredOptions() {
    Set<ConfigOption<?>> options = getFactory().requiredOptions();
    assert (options.size() == 1);
    assert (options.contains(AWS_REGION));
  }

  @Test
  void optionalOptions() {
    Set<ConfigOption<?>> options = getFactory().optionalOptions();
    assert (options.size() == 8);
    assert (!options.contains(AWS_REGION));
  }

  @Test
  void forwardOptions() {
    Set<ConfigOption<?>> options = getFactory().forwardOptions();
    assert (options.size() == 9);
  }

  // test utils

  AvroRowDataSerializationSchema getSerializer(String transportName, Map<String, Object> configs) {
    return new AvroRowDataSerializationSchema(
        ROW_TYPE,
        GlueAvroSerializationSchema.forGeneric(
            AvroSchemaConverter.convertToSchema(ROW_TYPE, MY_SCHEMA_NAME), transportName, configs),
        RowDataToAvroConverters.createConverter(ROW_TYPE));
  }

  AvroRowDataDeserializationSchema getDeserializer(Map<String, Object> configs) {
    return new AvroRowDataDeserializationSchema(
        GlueAvroDeserializationSchema.forGeneric(
            AvroSchemaConverter.convertToSchema(ROW_TYPE, MY_SCHEMA_NAME), configs),
        AvroToRowDataConverters.createRowConverter(ROW_TYPE),
        InternalTypeInfo.of(ROW_TYPE));
  }

  AvroGlueFormatFactory getFactory() {
    return new AvroGlueFormatFactory();
  }
}
