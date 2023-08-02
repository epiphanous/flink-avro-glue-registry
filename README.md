# Avro-Glue Format

> **THIS IS NOT AN OFFICIAL AWS GLUE LIBRARY**

This library supports using AWS Glue Schema Registry encoded messages in Flink SQL code.

The Avro-Glue format allows you to serialize/deserialize records via the AWS
Glue's `com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSchemaCoder` library. This can
only be used
in conjunction with the Apache Kafka SQL connector and Upsert Kafka SQL connector.

## Dependencies

In order to use the Avro-Glue format, the following dependencies are required for both projects using a build automation
tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

```
  <dependency>
    <groupId>io.epiphanous</groupId>
    <artifactId>flink-avro-glue-registry</artifactId>
    <version>1.0.0</version>
  </dependency>
  
  <dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-flink-serde</artifactId>
    <version>${glue.version}</version>
  </dependency>
```

## Create tables with Avro-Glue Format

```
CREATE TABLE user_created (
  id            STRING,
  name          STRING,
  email         STRING,
  primary key (id)
) WITH (
  'connector'                = 'kafka',
  'topic'                    = 'user_events',
  'properties.bootstrap.servers' 
                             = 'localhost:9092',
    
  'format'                   = 'avro-glue',
  'avro-glue.region'         = 'us-east-1',
  'avro-glue.registry'       = 'my-glue-registry',
  'avro-glue.transport.name' = 'user_events'
)
```

## Format Options

| Option                                  | Required | Forwarded |      Default       |  Type   | Description                                                     |
|-----------------------------------------|:--------:|:---------:|:------------------:|:-------:|-----------------------------------------------------------------|
| avro-glue.transport.name                |    no    |    yes    |                    | string  | computed from table name                                        |
| avro-glue.region                        |   yes    |    yes    |                    | string  |                                                                 |
| avro-glue.endpoint                      |    no    |    yes    |                    | string  | inferred from region                                            |
| avro-glue.registry.name                 |    no    |    yes    | `default-registry` | string  |                                                                 |
| avro-glue.schemaAutoRegistrationEnabled |    no    |    yes    |      `false`       | boolean |                                                                 |
| avro-glue.schemaName                    |    no    |    yes    |                    | string  |                                                                 |
| avro-glue.schemaNameGenerationClass     |    no    |    yes    |                    | string  | if provided, class name used to generate schema name at runtime |  
| avro-glue.secondaryDeserializer         |    no    |    yes    |                    | string  | if provided, class name used as glue secondary deserializer     |
| avro-glue.properties                    |    no    |    yes    |                    |   map   | will be passed on to aws glue serde as properties               |

Note that all options can be prefixed with `avro-glue.schema.registry` and all camel cased options can be dot
cased (`avro-glue.schema.auto.registration.enabled` is equivalent to `avro-glue.schemaAutoRegistrationEnabled`)
