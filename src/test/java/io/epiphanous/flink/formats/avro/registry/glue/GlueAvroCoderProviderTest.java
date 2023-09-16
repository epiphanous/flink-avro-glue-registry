package io.epiphanous.flink.formats.avro.registry.glue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GlueAvroCoderProviderTest {

  @Test
  void testEquals() {
    Map<String, Object> configA = new HashMap<>();
    configA.put("test", "1000");
    Map<String, Object> configB = new HashMap<>();
    configB.put("test", "1000");
    Map<String, Object> configC = new HashMap<>();
    configC.put("test", "5000");

    GlueAvroCoderProvider providerA = new GlueAvroCoderProvider("transport", configA);
    GlueAvroCoderProvider providerB = new GlueAvroCoderProvider("transport", configB);
    GlueAvroCoderProvider providerC = new GlueAvroCoderProvider("transport", configC);
    GlueAvroCoderProvider providerD = new GlueAvroCoderProvider("other transport", configB);

    assertThat(providerA).isEqualTo(providerB);
    assertThat(providerA).isNotEqualTo(providerC);
    assertThat(providerA).isNotEqualTo(providerD);
  }
}
