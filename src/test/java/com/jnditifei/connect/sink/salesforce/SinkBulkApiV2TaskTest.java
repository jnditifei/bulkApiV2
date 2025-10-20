package com.jnditifei.connect.sink.salesforce;

import com.jnditifei.salesforces.bulkv2.Bulk2Client;
import com.jnditifei.salesforces.bulkv2.response.CreateJobResponse;
import com.jnditifei.salesforces.bulkv2.response.GetJobInfoResponse;
import com.jnditifei.salesforces.bulkv2.type.OperationEnum;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.StringReader;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SinkBulkApiV2TaskTest {

    private SinkBulkApiV2Task task;
    private Bulk2Client client;

    @BeforeEach
    void setup() {
        task = new SinkBulkApiV2Task();
        client = mock(Bulk2Client.class);

        // Inject the mocked client using reflection (since it's private)
        try {
            var field = SinkBulkApiV2Task.class.getDeclaredField("client");
            field.setAccessible(true);
            field.set(task, client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Set minimal config
        Map<String, String> props = new HashMap<>();
        props.put("object.name", "Account");
        props.put("operation", "INSERT");

        try {
            var fieldObj = SinkBulkApiV2Task.class.getDeclaredField("objectName");
            var fieldOp = SinkBulkApiV2Task.class.getDeclaredField("operationStr");
            fieldObj.setAccessible(true);
            fieldOp.setAccessible(true);
            fieldObj.set(task, "Account");
            fieldOp.set(task, "INSERT");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPutAndFlushSingleRecord_MapValue() throws Exception {
        // Mock Salesforce job lifecycle
        CreateJobResponse createResp = mock(CreateJobResponse.class);
        when(createResp.getId()).thenReturn("JOB-1");

        GetJobInfoResponse jobInfo = mock(GetJobInfoResponse.class);
        when(jobInfo.isFinished()).thenReturn(true);

        when(client.createJob(anyString(), eq(OperationEnum.INSERT))).thenReturn(createResp);
        when(client.getJobInfo("JOB-1")).thenReturn(jobInfo);
        when(client.getJobFailedRecordResults("JOB-1")).thenReturn(new StringReader(""));

        // Prepare record
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("Name", "Acme Inc");
        map.put("Industry", "Tech");
        System.out.println(map);
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, map, 0);

        // Call put
        task.put(Collections.singletonList(record));

        // Manually flush (since batching thresholds may not trigger automatically)
        var flush = SinkBulkApiV2Task.class.getDeclaredMethod("flushBufferToSalesforce");
        flush.setAccessible(true);
        flush.invoke(task);

        // Capture the actual CSV string
        ArgumentCaptor<String> csvCaptor = ArgumentCaptor.forClass(String.class);
        verify(client).uploadJobData(eq("JOB-1"), csvCaptor.capture());

        String csv = csvCaptor.getValue();
        System.out.println(csv);

        // Assert the CSV is properly formatted
        // The map keys are sorted alphabetically (Industry, Name)
        String expectedHeader = "Industry,Name";
        String expectedLine = "Tech,Acme Inc";

        assertNotNull(csv);
        assertTrue(csv.startsWith(expectedHeader + "\n"), "CSV should start with header + newline");
        assertTrue(csv.contains(expectedLine), "CSV should contain correctly ordered data line");


        // Verify Salesforce job lifecycle
        verify(client).createJob(eq("Account"), eq(OperationEnum.INSERT));
        verify(client).uploadJobData(eq("JOB-1"), contains("Industry,Name"));
        verify(client).closeJob(eq("JOB-1"));
        verify(client).getJobInfo(eq("JOB-1"));
    }

    @Test
    void testPutWithStructValue() throws Exception {
        // Schema-based record
        Schema schema = SchemaBuilder.struct()
                .field("Name", Schema.STRING_SCHEMA)
                .field("Industry", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("Name", "Beta Ltd")
                .put("Industry", "Manufacturing");

        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, struct, 0);

        // Mock responses
        CreateJobResponse createResp = mock(CreateJobResponse.class);
        when(createResp.getId()).thenReturn("JOB-2");
        when(client.createJob(anyString(), eq(OperationEnum.INSERT))).thenReturn(createResp);

        GetJobInfoResponse jobInfo = mock(GetJobInfoResponse.class);
        when(jobInfo.isFinished()).thenReturn(true);
        when(client.getJobInfo("JOB-2")).thenReturn(jobInfo);
        when(client.getJobFailedRecordResults("JOB-2")).thenReturn(new StringReader(""));

        // Call put and flush
        task.put(Collections.singletonList(record));

        var flush = SinkBulkApiV2Task.class.getDeclaredMethod("flushBufferToSalesforce");
        flush.setAccessible(true);
        flush.invoke(task);

        ArgumentCaptor<String> csvCaptor = ArgumentCaptor.forClass(String.class);
        verify(client).uploadJobData(eq("JOB-2"), csvCaptor.capture());

        String csv = csvCaptor.getValue();
        assertTrue(csv.contains("Industry,Name"));
        assertTrue(csv.contains("Manufacturing"));
        assertTrue(csv.contains("Beta Ltd"));
    }

    @Test
    void testCsvEscapeHandlesQuotesAndCommas() throws Exception {
        var method = SinkBulkApiV2Task.class.getDeclaredMethod("csvEscape", String.class);
        method.setAccessible(true);

        String input = "ACME, \"Tech\"";
        String escaped = (String) method.invoke(null, input);

        assertTrue(escaped.startsWith("\""));
        assertTrue(escaped.endsWith("\""));
        assertTrue(escaped.contains("\"\"Tech\"\""));
    }

    @Test
    void testStopFlushesRemainingBuffer() throws Exception {
        // mock job lifecycle
        CreateJobResponse createResp = mock(CreateJobResponse.class);
        when(createResp.getId()).thenReturn("JOB-3");
        GetJobInfoResponse jobInfo = mock(GetJobInfoResponse.class);
        when(jobInfo.isFinished()).thenReturn(true);
        when(client.createJob(anyString(), eq(OperationEnum.INSERT))).thenReturn(createResp);
        when(client.getJobInfo("JOB-3")).thenReturn(jobInfo);
        when(client.getJobFailedRecordResults("JOB-3")).thenReturn(new StringReader(""));

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("Name", "Gamma LLC");
        map.put("Industry", "Finance");

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, map, 0);

        task.put(Collections.singletonList(record));
        task.stop();

        verify(client).createJob(eq("Account"), eq(OperationEnum.INSERT));
        verify(client).closeJob(anyString());
    }
}
