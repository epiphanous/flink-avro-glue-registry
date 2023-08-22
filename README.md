# Avro-Glue Format

> NOTE: This is **NOT** an official AWS Glue Library.

This library supports using AWS Glue Schema Registry encoded messages in Flink SQL code.

The Avro-Glue format allows you to serialize and deserialize records via AWS
Glue's `com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSchemaCoder` library. This can
only be used in conjunction with the Apache Kafka SQL connector and Upsert Kafka SQL connector.

## Dependencies

In order to use the Avro-Glue format, the following dependencies are required for both projects using a build automation
tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

```
  <dependency>
    <groupId>io.epiphanous</groupId>
    <artifactId>flink-avro-glue-registry</artifactId>
    <version>${avro-glue.version}</version>
  </dependency>
  
  <dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-flink-serde</artifactId>
    <version>${glue.version}</version>
  </dependency>
```

The AWS `schema-registry-flink-serde` dependency is required because the Avro-Glue format relies on AWS' official
`GlueSchemaRegistryAvroSchemaCoder` to interact with AWS Glue.

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
  'avro-glue.schemaName'     = 'org.example.avro.UserEvent'
  'avro-glue.region'         = 'us-east-1',
  'avro-glue.registry'       = 'my-glue-registry',
  'avro-glue.transport.name' = 'user_events'
)
```

### Format Options

| Option                                  | Required | Forwarded |      Default       |  Type   | Description                                                                |
|-----------------------------------------|:--------:|:---------:|:------------------:|:-------:|----------------------------------------------------------------------------|
| format                                  |   yes    |    no     |                    | string  | must be `avro-glue`                                                        |
| avro-glue.schemaName                    |   yes    |    yes    |                    | string  | the fully namespaced schema name (should match specific record class name) |
| avro-glue.region                        |    no    |    yes    |     us-east-1      | string  | aws region your glue registry is in                                        |
| avro-glue.endpoint                      |    no    |    yes    |                    | string  | inferred from region but useful for localstack testing                     |
| avro-glue.registry.name                 |    no    |    yes    | `default-registry` | string  | name of the glue registry                                                  |
| avro-glue.schemaAutoRegistrationEnabled |    no    |    yes    |      `false`       | boolean | if true, auto registers missing schemas on serialization                   |
| avro-glue.schemaNameGenerationClass     |    no    |    yes    |                    | string  | if provided, class name used to generate schema name at runtime            |  
| avro-glue.secondaryDeserializer         |    no    |    yes    |                    | string  | if provided, class name used as glue secondary deserializer                |
| avro-glue.properties                    |    no    |    yes    |                    |   map   | will be passed on to aws glue serde as properties                          |

Note that all options can be prefixed with `avro-glue.schema.registry` and all camel cased options can be dot
cased (`avro-glue.schema.auto.registration.enabled` is equivalent to `avro-glue.schemaAutoRegistrationEnabled`).

## Create tables with Debezium-Avro-Glue Format

`debezium-avro-glue` is a wrapper around the `avro-glue` format that allows you to parse Debezium encoded messages. Debezium support is heavily inspired by `debezium-avro-confluent` and `debezium-avro-json` formats.

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
    
  'format'                   = 'debezium-avro-glue',
  'avro-glue.schemaName'     = 'org.example.avro.UserEvent'
  'avro-glue.region'         = 'us-east-1',
  'avro-glue.registry'       = 'my-glue-registry',
  'avro-glue.transport.name' = 'user_events'
)
```

Format options are the same as the `avro-glue` format.


