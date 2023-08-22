package io.epiphanous.flink.formats.avro.registry.glue.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.epiphanous.flink.formats.avro.registry.glue.AvroGlueFormatOptions.*;
import static io.epiphanous.flink.formats.avro.registry.glue.debezium.DebeziumAvroGlueFormatFactory.IDENTIFIER;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DebeziumAvroGlueFormatFactory}. */
class DebeziumAvroGlueFormatFactoryTest {

  private static final ResolvedSchema SCHEMA = ResolvedSchema.of(
      Column.physical("a", DataTypes.STRING()),
      Column.physical("b", DataTypes.INT()),
      Column.physical("c", DataTypes.BOOLEAN()));

  private static final RowType ROW_TYPE = (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

  private static final String MY_SCHEMA_NAME = "debezium_test_schema";

  @Test
  void testDeserializationSchema() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_NAME.key(), MY_SCHEMA_NAME);
    configs.put(AWS_REGION.key(), AWS_REGION.defaultValue());

    final DebeziumAvroGlueDeserializationSchema expectedDeser = new DebeziumAvroGlueDeserializationSchema(
        ROW_TYPE,
        InternalTypeInfo.of(ROW_TYPE),
        MY_SCHEMA_NAME,
        configs);
    final DynamicTableSource actualSource = FactoryMocks.createTableSource(SCHEMA, getDefaultOptions());

    assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);

    TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock = (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
    DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat.createRuntimeDecoder(
        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

    assertThat(actualDeser).isEqualTo(expectedDeser);
  }

  @Test
  void testSerializationSchema() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_NAME.key(), MY_SCHEMA_NAME);
    configs.put(AWS_REGION.key(), AWS_REGION.defaultValue());

    final SerializationSchema<RowData> expectedSer = new DebeziumAvroGlueSerializationSchema(
        ROW_TYPE,
        MY_SCHEMA_NAME,
        configs);
    final DynamicTableSink actualSink = FactoryMocks.createTableSink(SCHEMA, getDefaultOptions());

    assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);

    TestDynamicTableFactory.DynamicTableSinkMock sinkMock = (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

    SerializationSchema<RowData> actualSer = sinkMock.valueFormat.createRuntimeEncoder(null,
        SCHEMA.toPhysicalRowDataType());

    assertThat(actualSer).isEqualTo(expectedSer);
  }

  @NotNull
  private Map<String, String> getDefaultOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", TestDynamicTableFactory.IDENTIFIER);
    options.put("target", "MyTarget");
    options.put("buffer-size", "1000");
    options.put("format", IDENTIFIER);
    options.put("debezium-avro-glue.topic", "test-topic");
    options.put("debezium-avro-glue.schemaName", MY_SCHEMA_NAME);
    return options;
  }
}