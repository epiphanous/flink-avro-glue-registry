package io.epiphanous.flink.formats.avro.registry.glue;

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSchemaCoder;
import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSchemaCoderProvider;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.formats.avro.SchemaCoder;

/**
 * A testable alternative to {@link GlueSchemaRegistryAvroSchemaCoderProvider} which provides a
 * proper equals implementation.
 */
public class GlueAvroCoderProvider implements SchemaCoder.SchemaCoderProvider {

  protected final String transportName;
  protected final Map<String, Object> configs;

  public GlueAvroCoderProvider(String transportName, Map<String, Object> configs) {
    this.transportName = transportName;
    this.configs = configs;
  }

  public GlueAvroCoderProvider(Map<String, Object> configs) {
    this.transportName = null;
    this.configs = configs;
  }

  public String getTransportName() {
    return transportName;
  }

  public Map<String, Object> getConfigs() {
    return configs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GlueAvroCoderProvider that = (GlueAvroCoderProvider) o;
    return Objects.equals(transportName, that.transportName)
        && Objects.equals(configs, that.configs);
  }

  @Override
  public SchemaCoder get() {
    return new GlueSchemaRegistryAvroSchemaCoder(transportName, configs);
  }
}
