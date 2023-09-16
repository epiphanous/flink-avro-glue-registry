package io.epiphanous.flink.formats.avro.registry.glue;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

/**
 * A similar implementation to
 * {@link
 * com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSerializationSchema} to
 * create a generic glue serializer implementation, leveraging our thin wrapper
 * {@link GlueAvroCoderProvider}. This makes it possible to create a working glue schema registry
 * serializer that can be tested.
 *
 * @param <T> The class of the records for this deserialization schema
 */
public class GlueAvroSerializationSchema<T> extends RegistryAvroSerializationSchema<T> {

  private GlueAvroSerializationSchema(
      Class<T> recordClazz, Schema schema, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
    super(recordClazz, schema, schemaCoderProvider);
  }

  public static GlueAvroSerializationSchema<GenericRecord> forGeneric(
      Schema schema, String transportName, Map<String, Object> configs) {
    return new GlueAvroSerializationSchema<>(
        GenericRecord.class, schema, new GlueAvroCoderProvider(transportName, configs));
  }
}
