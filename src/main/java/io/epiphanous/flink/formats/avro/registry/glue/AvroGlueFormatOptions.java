package io.epiphanous.flink.formats.avro.registry.glue;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for Glue Schema Registry Avro format. */
public class AvroGlueFormatOptions {

   static String dotCase(String name) {
    return name.replaceAll(
            String.format(
                "%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])", "(?<=[^A-Z])(?=[A-Z])", "(?<=[A-Za-z])(?=[^A-Za-z])"),
            ".")
        .toLowerCase();
  }

  public static final ConfigOption<String> TRANSPORT_NAME =
      ConfigOptions.key("transport.name")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The name of the message transport (for Kafka, the topic). Normally inferred from the table identifier.");

  public static final ConfigOption<String> AWS_REGION =
      ConfigOptions.key(AWSSchemaRegistryConstants.AWS_REGION)
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("aws.region")
          .withDescription("The AWS Region your Glue schema registry operates in.");

  public static final ConfigOption<String> AWS_ENDPOINT =
      ConfigOptions.key(AWSSchemaRegistryConstants.AWS_ENDPOINT)
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("aws.endpoint")
          .withDescription(
              "The AWS Glue endpoint. Normally inferred "
                  + "automatically from your AWS Region setting, but useful for "
                  + "testing with localstack.");

  public static final ConfigOption<String> REGISTRY_NAME =
      ConfigOptions.key(AWSSchemaRegistryConstants.REGISTRY_NAME)
          .stringType()
          .defaultValue(AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME)
          .withFallbackKeys("schema.registry." + AWSSchemaRegistryConstants.REGISTRY_NAME)
          .withDescription(
              "The name of the Flue registry to operate on. "
                  + "Defaults"
                  + " to "
                  + AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME
                  + ".");

  public static final ConfigOption<String> SCHEMA_AUTO_REGISTRATION_SETTING =
      ConfigOptions.key(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING)
          .stringType()
          .defaultValue("false")
          .withFallbackKeys(
              dotCase(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING),
              "schema.registry." + AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
              "schema.registry."
                  + dotCase(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING))
          .withDescription(
              "If true, schemas missing from the registry will be "
                  + "auto-registered on serialization. Default is false.");

  public static final ConfigOption<String> SCHEMA_NAME =
      ConfigOptions.key(AWSSchemaRegistryConstants.SCHEMA_NAME)
          .stringType()
          .noDefaultValue()
          .withFallbackKeys(
              dotCase(AWSSchemaRegistryConstants.SCHEMA_NAME),
              "schema.registry." + AWSSchemaRegistryConstants.SCHEMA_NAME,
              "schema.registry." + dotCase(AWSSchemaRegistryConstants.SCHEMA_NAME))
          .withDescription(
              "The schema name to register the schema under. If not "
                  + "provide, defaults to the transport name (topic name for Kafka).");

  public static final ConfigOption<String> SCHEMA_NAMING_GENERATION_CLASS =
      ConfigOptions.key(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS)
          .stringType()
          .noDefaultValue()
          .withFallbackKeys(
              dotCase(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS),
              "schema.registry." + AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
              "schema.registry."
                  + dotCase(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS))
          .withDescription(
              "The name of a class implementing "
                  + "`AWSSchemaNamingStrategy` that will be used to dynamically "
                  + "determine the name of the schema for each data object being "
                  + "serialized.");
  public static final ConfigOption<String> SECONDARY_DESERIALIZER =
      ConfigOptions.key(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)
          .stringType()
          .noDefaultValue()
          .withFallbackKeys(
              dotCase(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER),
              "schema.registry." + AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER,
              "schema.registry." + dotCase(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER))
          .withDescription(
              "The class name of a fallback deserializer that will "
                  + "be used when Glue can't decode a message. Useful for transitioning"
                  + " from another schema registry, like Confluent.");

  public static final ConfigOption<Map<String, String>> PROPERTIES =
      ConfigOptions.key("properties")
          .mapType()
          .noDefaultValue()
          .withFallbackKeys("schema.registry.properties")
          .withDescription(
              "Properties map that is forwarded to the underlying "
                  + "Schema Registry. This is useful for options that are not "
                  + "officially exposed via Flink config options. However, note that "
                  + "Flink options have higher precedence.");

  private AvroGlueFormatOptions() {}
}
