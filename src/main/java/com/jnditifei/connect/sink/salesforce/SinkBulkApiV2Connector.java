package com.jnditifei.connect.sink.salesforce;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkBulkApiV2Connector extends SinkConnector {

    public static final String CLIENT_ID = "client.id";
    public static final String CLIENT_SECRET = "client.secret";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String OBJECT_NAME = "object.name"; // e.g. Account
    public static final String OPERATION = "operation"; // INSERT / UPDATE / UPSERT / DELETE
    public static final String EXTERNAL_ID = "external.id.field"; // optional for upsert
    public static final String TOKEN_REQUEST_ENDPOINT = "token.request.endpoint";

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
                .define(TOKEN_REQUEST_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Salesforce login URL")
                .define(OPERATION, ConfigDef.Type.STRING, "INSERT", ConfigDef.Importance.MEDIUM, "Operation: INSERT|UPDATE|UPSERT|DELETE")
                .define(EXTERNAL_ID, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "External ID field for UPSERT");
    }

    @Override
    public String version() {
        return "0.0.1-SNAPSHOT";
    }
}
