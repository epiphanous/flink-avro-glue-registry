package io.epiphanous.flink.formats.avro.registry.glue.debezium;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class DebeziumAvroUtils {

  public static RowType createDebeziumAvroRowType(DataType dataType) {
    // Debezium Avro contains other information, e.g. "source", "ts_ms"
    // but we don't need them
    return (RowType)
        DataTypes.ROW(
                DataTypes.FIELD("before", dataType.nullable()),
                DataTypes.FIELD("after", dataType.nullable()),
                DataTypes.FIELD("op", DataTypes.STRING()))
            .getLogicalType();
  }
}
