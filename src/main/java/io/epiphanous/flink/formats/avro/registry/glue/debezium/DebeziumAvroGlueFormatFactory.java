package io.epiphanous.flink.formats.avro.registry.glue.debezium;

import io.epiphanous.flink.formats.avro.registry.glue.AbstractAvroGlueFormatFactory;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumAvroGlueFormatFactory extends AbstractAvroGlueFormatFactory {

  public static final String IDENTIFIER = "debezium-avro-glue";
  private static final Logger LOG = LoggerFactory.getLogger(DebeziumAvroGlueFormatFactory.class);

  @Override
  protected ChangelogMode changelogMode() {
    return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_BEFORE).addContainedKind(RowKind.UPDATE_AFTER)
        .addContainedKind(RowKind.DELETE).build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected DeserializationSchema<RowData> getDeserializationSchema(RowType rowType,
      TypeInformation<RowData> rowDataTypeInfo, String schemaName, Map<String, Object> configs) {
    return new DebeziumAvroGlueDeserializationSchema(rowType, rowDataTypeInfo, schemaName, configs);
  }

  @Override
  protected SerializationSchema<RowData> getSerializationSchema(RowType rowType, String schemaName,
      String topic, Map<String, Object> configs) {
    return new DebeziumAvroGlueSerializationSchema(rowType, schemaName, configs);
  }
}
