# Salesforce Bulk API v2 Kafka Sink Connector

### Overview

The Salesforce Bulk API v2 Kafka Sink Connector is a Kafka Connect plugin that allows you to stream data from Kafka topics directly into Salesforce using the Bulk API v2 interface.
It supports high-volume upserts and inserts into Salesforce objects, with configurable batching, error handling, and job monitoring.

This connector is designed for efficient integration between Kafka and Salesforce, especially for workloads that generate large batches of records.

### Features

Supports INSERT and UPSERT operations via Salesforce Bulk API v2

Dynamic batching with configurable thresholds:

Maximum batch size (in bytes)

Maximum record count

Maximum time interval

Automatic Salesforce job creation, data upload, and job closure

Error handling with optional routing of:

Failed records to a dedicated topic

Unprocessed records to a separate topic

Configurable API version, OAuth credentials, and object name

Compatible with Confluent Platform and Confluent Cloud

#### Architecture

Kafka Topic  →  SalesforceBulkSinkTask  →  Bulk2Client  →  Salesforce Bulk API v2

Class Relationships

SalesforceSinkConnector

Defines the connector configuration and creates task instances.

SalesforceSinkTask

Core logic for processing Kafka records and sending them to Salesforce.

Bulk2Client

Handles Salesforce Bulk API v2 operations (create job, upload data, close job, etc.).

RestRequester

Generic HTTP utility to interact with Salesforce endpoints.

AccessToken & Bulk2ClientBuilder

Manage authentication, token renewal, and HTTP client setup.

#### Configuration

Example connector configuration:

```javascript
{
  "name": "salesforce-bulk-v2-sink",
  "config": {
    "connector.class": "com.jnditifei.connect.sink.salesforce.SalesforceSinkConnector",
    "tasks.max": "1",
    "topics": "salesforce_records",

    "salesforce.object": "CameleonCPQ__QxQuoteLine__c",
    "salesforce.operation": "UPSERT",
    "salesforce.external.id.field": "CameleonCPQ__QuoteId__r.ExternalId__c",

    "salesforce.token.endpoint": "https://login.salesforce.com/services/oauth2/token",
    "salesforce.consumer.key": "yourConsumerKey",
    "salesforce.consumer.secret": "yourConsumerSecret",
    "salesforce.username": "yourUsername",
    "salesforce.password": "yourPassword",

    "batch.max.bytes": "5000000",
    "batch.max.records": "10000",
    "batch.max.interval.ms": "15000",

    "api.version": "v59.0",

    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.schemas.enable": "false",
    "value.converter.schema.registry.url": "https://your-schema-registry-url",
    "value.converter.basic.auth.credentials.source": "USER_INFO",
    "value.converter.basic.auth.user.info": "username:password"
  }
}
```

### How It Works

Kafka Connect delivers records to the connector.

The task converts incoming records into CSV format (header + rows).

Records are buffered until:

Maximum record count, byte size, or time interval is reached.

A header change is detected.

When any flush condition is met:

The connector creates a Salesforce Bulk API job.

Uploads the CSV data.

Closes the job and polls until completion.

Once completed:

Failed records are optionally published to a DLQ topic.

Unprocessed records are routed to another topic if available.

Error Handling

Failed Records
When Salesforce returns failed records for a completed job, they are logged and optionally sent to a dedicated topic.

Unprocessed Records
If a job fails entirely (e.g., malformed data, invalid field), the unprocessed batch can be routed to another Kafka topic for review.

Logging and Monitoring

Typical successful job logs include:

```javascript
INFO  [Salesforce] Created Bulk API v2 job: 7505g00000XYZ123 for object=CameleonCPQ__QxQuoteLine__c operation=UPSERT
INFO  [Salesforce] Uploaded 500 records to job 7505g00000XYZ123
INFO  [Salesforce] Closed job 7505g00000XYZ123
INFO  [Salesforce] Job 7505g00000XYZ123 completed with state=JobComplete processed=500 failed=0
```

#### Requirements

Salesforce account with API access and Bulk API v2 enabled

Kafka Connect (standalone or distributed)

Confluent Schema Registry (if using JSON or Avro converters)

Java 11+

Building and Running
mvn clean package


Deploy the connector JAR in your Kafka Connect plugin path and register the connector configuration via REST API:

```javascript
curl -X POST -H "Content-Type: application/json" \
  --data @config.json \
  http://localhost:8083/connectors
```