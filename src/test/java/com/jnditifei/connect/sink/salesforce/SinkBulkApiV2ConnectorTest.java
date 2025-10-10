package com.jnditifei.connect.sink.salesforce;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SinkBulkApiV2ConnectorTest {

    private SinkBulkApiV2Connector connector;

    @BeforeEach
    void setUp() {
        connector = new SinkBulkApiV2Connector();
    }

    @Test
    void testVersionIsDefined() {
        assertEquals("0.0.1-SNAPSHOT", connector.version());
    }

    @Test
    void testStartStoresProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(SinkBulkApiV2Connector.CLIENT_ID, "abc123");
        props.put(SinkBulkApiV2Connector.CLIENT_SECRET, "secret");

        connector.start(props);

        List<Map<String, String>> configs = connector.taskConfigs(2);
        assertEquals(2, configs.size(), "Should return config for each task");
        assertEquals("abc123", configs.get(0).get(SinkBulkApiV2Connector.CLIENT_ID));
        assertEquals("secret", configs.get(1).get(SinkBulkApiV2Connector.CLIENT_SECRET));
    }

    @Test
    void testTaskClassIsSinkBulkApiV2Task() {
        Class<? extends Task> taskClass = connector.taskClass();
        assertEquals(SinkBulkApiV2Task.class, taskClass);
    }

    @Test
    void testConfigDefinitionContainsExpectedKeys() {
        ConfigDef configDef = connector.config();
        Set<String> keys = configDef.names();

        assertTrue(keys.contains(SinkBulkApiV2Connector.CLIENT_ID));
        assertTrue(keys.contains(SinkBulkApiV2Connector.CLIENT_SECRET));
        assertTrue(keys.contains(SinkBulkApiV2Connector.USERNAME));
        assertTrue(keys.contains(SinkBulkApiV2Connector.PASSWORD));
        assertTrue(keys.contains(SinkBulkApiV2Connector.OBJECT_NAME));
        assertTrue(keys.contains(SinkBulkApiV2Connector.OPERATION));
        assertTrue(keys.contains(SinkBulkApiV2Connector.EXTERNAL_ID));
        assertTrue(keys.contains(SinkBulkApiV2Connector.TOKEN_REQUEST_ENDPOINT));
    }

    @Test
    void testStopDoesNotThrow() {
        assertDoesNotThrow(() -> connector.stop());
    }
}
