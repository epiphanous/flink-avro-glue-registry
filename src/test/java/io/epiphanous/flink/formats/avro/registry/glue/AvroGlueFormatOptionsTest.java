package io.epiphanous.flink.formats.avro.registry.glue;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;

class AvroGlueFormatOptionsTest {

  @Test
  void dotCase() {
    assertThat(AvroGlueFormatOptions.dotCase(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS))
        .isEqualTo("schema.name.generation.class");
  }
}
