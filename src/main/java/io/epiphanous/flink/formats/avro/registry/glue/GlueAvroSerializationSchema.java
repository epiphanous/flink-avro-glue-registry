package io.epiphanous.flink.formats.avro.registry.glue;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

public class GlueAvroSerializationSchema<T> extends
    RegistryAvroSerializationSchema<T> {
  private GlueAvroSerializationSchema(Class<T> recordClazz, Schema schema,
                                     SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
    super(recordClazz, schema, schemaCoderProvider);
  }

  static public GlueAvroSerializationSchema<GenericRecord> forGeneric(Schema schema, String transportName, Map<String, Object> configs) {
    return new GlueAvroSerializationSchema<>(GenericRecord.class, schema, new GlueAvroCoderProvider(transportName, configs));
  }
}
