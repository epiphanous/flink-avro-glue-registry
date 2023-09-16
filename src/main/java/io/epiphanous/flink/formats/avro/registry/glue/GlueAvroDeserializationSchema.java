package io.epiphanous.flink.formats.avro.registry.glue;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;
import org.jetbrains.annotations.Nullable;

/**
 * A similar implementation to
 * {@link
 * com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema} to
 * create a generic glue deserializer implementation, leveraging our thin wrapper
 * {@link GlueAvroCoderProvider}. This makes it possible to create a working glue schema registry
 * deserializer that can be tested.
 *
 * @param <T> The class of the records for this deserialization schema
 */
public class GlueAvroDeserializationSchema<T> extends RegistryAvroDeserializationSchema<T> {

  private GlueAvroDeserializationSchema(
      Class<T> recordClazz,
      @Nullable Schema reader,
      SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
    super(recordClazz, reader, schemaCoderProvider);
  }

  public static GlueAvroDeserializationSchema<GenericRecord> forGeneric(
      Schema schema, Map<String, Object> configs) {
    return new GlueAvroDeserializationSchema<>(
        GenericRecord.class, schema, new GlueAvroCoderProvider(configs));
  }
}
