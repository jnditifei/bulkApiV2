package com.jnditifei.connect.sink.salesforce;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkBulkApiV2Connector extends SinkConnector {

    public static final String CLIENT_ID = "salesforce.client.id";
    public static final String CLIENT_SECRET = "salesforce.client.secret";
    public static final String USERNAME = "salesforce.username";
    public static final String PASSWORD = "salesforce.password";
    public static final String OBJECT_NAME = "salesforce.object.name"; // e.g. Account
    public static final String OPERATION = "salesforce.operation"; // INSERT / UPDATE / UPSERT / DELETE
    public static final String EXTERNAL_ID = "salesforce.external.id.field"; // optional for upsert
    public static final String TOKEN_REQUEST_ENDPOINT = "salesforce.token.request.endpoint";
    public static final String API_VERSION = "salesforce.api.version";
    public static final String BATCH_MAX_BYTES = "batch.max.bytes";
    public static final String BATCH_MAX_RECORDS = "batch.max.records";
    public static final String BATCH_FLUSH_INTERVAL_MS = "flush.interval.ms";
    public static final String JOB_POLL_INTERVAL_MS = "salesforce.poll.interval.ms";
    public static final String JOB_POLL_TIMEOUT_MS = "salesforce.poll.timeout.ms";

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SinkBulkApiV2Task.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProps));
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(CLIENT_ID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce connected app client id")
                .define(CLIENT_SECRET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce connected app client secret")
                .define(USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce username")
                .define(PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Salesforce password + security token")
                .define(OBJECT_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce object (e.g. Account)")
                .define(TOKEN_REQUEST_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce OAuth2 token endpoint (")
                .define(OPERATION, ConfigDef.Type.STRING, "INSERT", ConfigDef.Importance.MEDIUM, "Operation: INSERT|UPDATE|UPSERT|DELETE")
                .define(EXTERNAL_ID, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "External ID field for UPSERT operations")
                .define(API_VERSION, ConfigDef.Type.STRING, "v.60", ConfigDef.Importance.MEDIUM, "Salesforce REST API version")
                .define(BATCH_MAX_BYTES, ConfigDef.Type.INT, 1024 * 512, ConfigDef.Importance.LOW, "Maximum size per batch")
                .define(BATCH_MAX_RECORDS, ConfigDef.Type.INT, 1000, ConfigDef.Importance.LOW, "Max CSV payload size before flush")
                .define(BATCH_FLUSH_INTERVAL_MS, ConfigDef.Type.LONG, 10000L, ConfigDef.Importance.LOW, "Max time between flushes")
                .define(JOB_POLL_INTERVAL_MS, ConfigDef.Type.LONG, 5000L, ConfigDef.Importance.LOW, "Wait time between calling Salesforce result endpoint")
                .define(JOB_POLL_TIMEOUT_MS, ConfigDef.Type.LONG, 500000L, ConfigDef.Importance.LOW, "Max total wait time to get job result");
    }

    @Override
    public String version() {
        return "0.0.1-SNAPSHOT";
    }
}
