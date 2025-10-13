package com.jnditifei.connect.sink.salesforce;

import com.jnditifei.salesforces.bulkv2.Bulk2Client;
import com.jnditifei.salesforces.bulkv2.Bulk2ClientBuilder;
import com.jnditifei.salesforces.bulkv2.response.CreateJobResponse;
import com.jnditifei.salesforces.bulkv2.response.GetJobInfoResponse;
import com.jnditifei.salesforces.bulkv2.type.OperationEnum;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class SinkBulkApiV2Task extends SinkTask {
    // batching defaults
    private static final int BATCH_MAX_RECORDS = 1000;
    private static final int BATCH_MAX_BYTES = 1024 * 512; // 512 KB
    private static final long POLL_SLEEP_MS = 1000L;
    private static final long JOB_POLL_INTERVAL_MS = 2000L;
    private static final long JOB_POLL_TIMEOUT_MS = 60_000L; // 60s

    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
    private String tokenEndpoint;
    private String objectName;
    private String operationStr;
    private String externalIdField;

    private Bulk2Client client;

    // buffer for CSV lines (excluding header)
    private final StringBuilder csvBuffer = new StringBuilder();
    private int recordCount = 0;
    private int bufferedBytes = 0;
    private String csvHeader; // set once on first record

    @Override
    public String version() {
        return "0.0.1-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        clientId = props.get(SinkBulkApiV2Connector.CLIENT_ID);
        clientSecret = props.get(SinkBulkApiV2Connector.CLIENT_SECRET);
        username = props.get(SinkBulkApiV2Connector.USERNAME);
        password = props.get(SinkBulkApiV2Connector.PASSWORD);
        objectName = props.get(SinkBulkApiV2Connector.OBJECT_NAME);
        operationStr = props.getOrDefault(SinkBulkApiV2Connector.OPERATION, "INSERT").toUpperCase(Locale.ROOT);
        externalIdField = props.getOrDefault(SinkBulkApiV2Connector.EXTERNAL_ID, null);
        tokenEndpoint = props.get(SinkBulkApiV2Connector.TOKEN_REQUEST_ENDPOINT);

        // Build the Salesforce Bulk2Client using the library
        Bulk2ClientBuilder builder = new Bulk2ClientBuilder()
                .withPasswordAndTokenEndpoint(tokenEndpoint, clientId, clientSecret, username, password);
        try {
            client = builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) return;

        for (SinkRecord r : records) {
            // Convert record value to CSV header + line(s)
            // Implement valueToCsvLine to match your schema
            CsvLine csvLine = valueToCsvLine(r);
            if (csvLine == null) continue;

            if (csvHeader == null) {
                csvHeader = csvLine.header;
            }

            // ensure header matches (simple check)
            if (!csvHeader.equals(csvLine.header)) {
                // Incompatibility: flush current buffer first, then switch header
                flushBufferToSalesforce();
                csvHeader = csvLine.header;
            }

            csvBuffer.append(csvLine.line).append("\n");
            recordCount++;
            bufferedBytes += csvLine.line.getBytes().length + 1;

            if (recordCount >= BATCH_MAX_RECORDS || bufferedBytes >= BATCH_MAX_BYTES) {
                flushBufferToSalesforce();
            }
        }
    }

    @Override
    public void stop() {
        // flush remaining records
        flushBufferToSalesforce();
        // close client if it supports close (not strictly necessary)
        // if client has close/logout, call it here
    }

    // Minimal helper to convert record to CSV header+line. You MUST adapt this to your record schema.
    private static class CsvLine {
        final String header;
        final String line;
        CsvLine(String header, String line) { this.header = header; this.line = line; }
    }

    /**
     * Convert SinkRecord to CSV header + line.
     *
     * This example supports:
     *  - value is Map<String,Object> -> keys order is alphabetical (change as needed)
     *  - value is String -> treated as a single column "value"
     *  - value is Struct -> you can extract fields similarly (not shown)
     *
     * You should adapt this to your record schema (Struct, JSON string, Avro, etc).
     */
    private CsvLine valueToCsvLine(SinkRecord r) {
        if (r == null) return null;
        Object val = r.value();

        // --- Case 1: Avro/Schema-based data (Struct) ---
        // --- Case 1: Avro/Schema-based data (Struct) ---
        if (val instanceof org.apache.kafka.connect.data.Struct struct) {
            List<String> keys = struct.schema().fields()
                    .stream()
                    .map(org.apache.kafka.connect.data.Field::name)
                    .sorted()
                    .toList();

            String header = String.join(",", keys);
            StringBuilder row = new StringBuilder();

            for (int i = 0; i < keys.size(); i++) {
                Object fieldValue = struct.get(keys.get(i));
                row.append(csvEscape(String.valueOf(fieldValue == null ? "" : fieldValue)));
                if (i < keys.size() - 1) row.append(",");
            }

            return new CsvLine(header, row.toString());
        }

        // --- Case 2: JSON-style Map ---
        else if (val instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) val;
            List<String> keys = new ArrayList<>(map.keySet());
            Collections.sort(keys);
            String header = String.join(",", keys);

            StringBuilder row = new StringBuilder();
            for (int i = 0; i < keys.size(); i++) {
                Object v = map.get(keys.get(i));
                row.append(csvEscape(String.valueOf(v == null ? "" : v)));
                if (i < keys.size() - 1) row.append(",");
            }
            return new CsvLine(header, row.toString());
        }

        // --- Case 3: Plain String ---
        else if (val instanceof String str) {
            String header = "value";
            String line = csvEscape(str);
            return new CsvLine(header, line);
        }

        // --- Case 4: Fallback for primitives or unknown types ---
        else {
            String header = "value";
            String line = csvEscape(String.valueOf(val));
            return new CsvLine(header, line);
        }
    }

    private static String csvEscape(String s) {
        if (s == null) return "";
        // minimal escaping: wrap in quotes if contains comma or quote or newline
        if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")) {
            String esc = s.replace("\"", "\"\"");
            return "\"" + esc + "\"";
        }
        return s;
    }

    private synchronized void flushBufferToSalesforce() {
        if (recordCount == 0 || client == null || objectName == null || csvHeader == null) return;

        // Build CSV: header + rows
        StringBuilder fullCsv = new StringBuilder();
        fullCsv.append(csvHeader).append("\n");
        fullCsv.append(csvBuffer.toString());

        try {
            // Determine operation enum
            OperationEnum op = OperationEnum.valueOf(operationStr);

            // Create job
            CreateJobResponse createResp;
            if (op == OperationEnum.UPSERT && externalIdField != null && !externalIdField.isEmpty()) {
                createResp = client.createJob(objectName, op, req -> req.withExternalIdFieldName(externalIdField));
            } else {
                createResp = client.createJob(objectName, op);
            }
            String jobId = createResp.getId();

            // Upload data (separate request)
            client.uploadJobData(jobId, fullCsv.toString());

            // Close job
            client.closeJob(jobId);

            // Poll for completion (simple loop with timeout)
            long started = System.currentTimeMillis();
            while (true) {
                GetJobInfoResponse info = client.getJobInfo(jobId);
                // Note: depending on library shapes, you may inspect jobInfo.getState() or jobInfo.isFinished()
                if (info.isFinished()) {
                    break;
                }
                if (System.currentTimeMillis() - started > JOB_POLL_TIMEOUT_MS) {
                    throw new RuntimeException("Salesforce job polling timed out for jobId=" + jobId);
                }
                try {
                    Thread.sleep(JOB_POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            // Optional: read results
            try (Reader r = client.getJobFailedRecordResults(jobId)) {
                BufferedReader br = new BufferedReader(r);
                br.lines().forEach(line -> {
                    // you may want to route failed lines to a DLQ or log them
                    log.error("Salesforce failed record: {}", line);
                });
            } catch (IOException e) {
                log.warn("Error reading failed records for job {}", jobId, e);
            }

        } catch (Exception e) {
            // DO NOT endlessly swallow exceptions: failing here will cause Connect to retry/mark tasks accordingly.
            throw new RuntimeException("Error flushing data to Salesforce Bulk API", e);
        } finally {
            // reset buffer
            csvBuffer.setLength(0);
            recordCount = 0;
            bufferedBytes = 0;
            // keep header so next batch expects same columns; if you want to drop header, set csvHeader=null
            // csvHeader = null;
        }
    }

    // placeholder logger: change to your logger (SLF4J)
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SinkBulkApiV2Task.class);
}
