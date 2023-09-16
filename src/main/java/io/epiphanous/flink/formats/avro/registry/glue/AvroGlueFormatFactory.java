package io.epiphanous.flink.formats.avro.registry.glue;

import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table format factory for providing configured instances of Glue Schema Avro Registry to RowData
 * {@link SerializationSchema} and {@link DeserializationSchema}.
 */
public class AvroGlueFormatFactory extends AbstractAvroGlueFormatFactory {

  public static final String IDENTIFIER = "avro-glue";
  private static final Logger LOG = LoggerFactory.getLogger(AvroGlueFormatFactory.class);

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  protected DeserializationSchema<RowData> getDeserializationSchema(
      RowType rowType,
      TypeInformation<RowData> rowDataTypeInfo,
      String schemaName,
      Map<String, Object> configs) {
    return new AvroRowDataDeserializationSchema(
        GlueAvroDeserializationSchema.forGeneric(
            AvroSchemaConverter.convertToSchema(rowType, schemaName), configs),
        AvroToRowDataConverters.createRowConverter(rowType),
        rowDataTypeInfo);
  }

  protected SerializationSchema<RowData> getSerializationSchema(
      RowType rowType, String schemaName, String topic, Map<String, Object> configs) {
    return new AvroRowDataSerializationSchema(
        rowType,
        GlueAvroSerializationSchema.forGeneric(
            AvroSchemaConverter.convertToSchema(rowType, schemaName), topic, configs),
        RowDataToAvroConverters.createConverter(rowType));
  }

  @Override
  protected ChangelogMode changelogMode() {
    return ChangelogMode.insertOnly();
  }
}
