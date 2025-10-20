package com.jnditifei.connect.sink.salesforce;

import com.jnditifei.salesforces.bulkv2.Bulk2Client;
import com.jnditifei.salesforces.bulkv2.Bulk2ClientBuilder;
import com.jnditifei.salesforces.bulkv2.response.CreateJobResponse;
import com.jnditifei.salesforces.bulkv2.response.GetJobInfoResponse;
import com.jnditifei.salesforces.bulkv2.type.OperationEnum;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class SinkBulkApiV2Task extends SinkTask {

    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
    private String tokenEndpoint;
    private String objectName;
    private String operationStr;
    private String externalIdField;
    private String apiVersion;
    private int batchMaxRecords;
    private int batchMaxBytes;
    private long batchMaxIntervalMs;
    private long lastFlushTime = System.currentTimeMillis();
    private long jobPollIntervalMs;
    private long jobPollTimeOutMs;

    private Bulk2Client client;

    // buffer for CSV lines (excluding header)
    private final StringBuilder csvBuffer = new StringBuilder();
    private int recordCount = 0;
    private int bufferedBytes = 0;
    private String csvHeader; // set once on first record

    @Override
    public String version() {
        return SinkBulkApiV2Connector.API_VERSION;
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
        apiVersion = props.getOrDefault(SinkBulkApiV2Connector.API_VERSION, "v60.0");
        batchMaxIntervalMs = Long.parseLong(props.getOrDefault(SinkBulkApiV2Connector.BATCH_MAX_BYTES, "10000"));
        batchMaxBytes = Integer.parseInt(props.getOrDefault(SinkBulkApiV2Connector.BATCH_MAX_BYTES, "524288"));
        batchMaxRecords = Integer.parseInt(props.getOrDefault(SinkBulkApiV2Connector.BATCH_MAX_RECORDS, "1000"));
        jobPollIntervalMs = Long.parseLong(props.getOrDefault(SinkBulkApiV2Connector.JOB_POLL_INTERVAL_MS, "5000"));
        jobPollTimeOutMs = Long.parseLong(props.getOrDefault(SinkBulkApiV2Connector.JOB_POLL_TIMEOUT_MS, "500000"));

        Bulk2ClientBuilder builder = new Bulk2ClientBuilder()
                .withPasswordAndTokenEndpoint(tokenEndpoint, clientId, clientSecret, username, password)
                .withApiVersion(apiVersion);
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
            CsvLine csvLine = valueToCsvLine(r);
            if (csvLine == null) continue;

            if (csvHeader == null) {
                csvHeader = csvLine.header;
            }

            if (!csvHeader.equals(csvLine.header)) {
                // Header changed → flush immediately
                flushBufferToSalesforce();
                csvHeader = csvLine.header;
            } else {
                // Same header → keep buffering and flush by size/time/count
                csvBuffer.append(csvLine.line).append("\n");
                recordCount++;
                bufferedBytes += csvLine.line.getBytes().length + 1;

                boolean sizeFull = bufferedBytes >= batchMaxBytes;
                boolean countFull = recordCount >= batchMaxRecords;

                if (sizeFull || countFull) {
                    flushBufferToSalesforce();
                }
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        long now = System.currentTimeMillis();
        boolean timeExceeded = (now - lastFlushTime) >= batchMaxIntervalMs;
        if (timeExceeded) {
            log.info("⏰ Time-based flush triggered via preCommit()");
            flushBufferToSalesforce();
        }
        return currentOffsets;
    }

    @Override
    public void stop() {
        // flush remaining records
        flushBufferToSalesforce();
    }

    private static class CsvLine {
        final String header;
        final String line;
        CsvLine(String header, String line) { this.header = header; this.line = line; }
    }

    private CsvLine valueToCsvLine(SinkRecord r) {
        if (r == null) return null;
        Object val = r.value();

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

        StringBuilder fullCsv = new StringBuilder();
        fullCsv.append(csvHeader).append("\n");
        fullCsv.append(csvBuffer.toString());

        try {
            OperationEnum op = OperationEnum.valueOf(operationStr);

            // Log start of batch
            log.info("Starting Salesforce Bulk API job for object={} operation={} with {} records",
                    objectName, op, recordCount);

            CreateJobResponse createResp;
            if (op == OperationEnum.UPSERT && externalIdField != null && !externalIdField.isEmpty()) {
                createResp = client.createJob(objectName, op, req -> req.withExternalIdFieldName(externalIdField));
            } else {
                createResp = client.createJob(objectName, op);
            }

            String jobId = createResp.getId();
            log.info("Created Salesforce job ID={} for object={}", jobId, objectName);

            client.uploadJobData(jobId, fullCsv.toString());
            log.info("Uploaded CSV data for job ID={} ({} bytes)", jobId, fullCsv.length());

            client.closeJob(jobId);
            log.info("Closed job ID={} – waiting for completion...", jobId);

            // Poll for job completion
            long started = System.currentTimeMillis();
            GetJobInfoResponse info = null;
            while (true) {
                info = client.getJobInfo(jobId);
                if (info.isFinished()) break;

                if (System.currentTimeMillis() - started > jobPollTimeOutMs) {
                    throw new RuntimeException("Salesforce job polling timed out for jobId=" + jobId);
                }
                Thread.sleep(jobPollIntervalMs);
            }

            log.info("Salesforce job completed: jobId={} object={} state={} processedRecords={} failedRecords={} duration={}ms",
                    jobId, objectName, info.getState(), info.getNumberRecordsProcessed(), info.getNumberRecordsFailed(), info.getTotalProcessingTime());

            /*
             * Check for unprocessed record results
             */
            if (info.getNumberRecordsFailed() > 0 )  {
                try (Reader r = client.getJobUnprocessedRecordResults(jobId)) {
                    BufferedReader br = new BufferedReader(r);
                    br.lines().forEach(line -> {
                        if (!line.trim().isEmpty()) {
                            log.error("Salesforce Unprocessed record for job {}: {}", jobId, line);
                        }
                    });
                } catch (IOException e) {
                    log.warn("Salesforce Error reading unprocessed records for job {}", jobId, e);
                }
            }

            /*
             * Check failed records
             */
            if ("JobComplete".equalsIgnoreCase(String.valueOf(info.getState()))
                    && info.getNumberRecordsFailed() > 0 || "Failed".equalsIgnoreCase(String.valueOf(info.getState()))) {
                try (Reader r = client.getJobFailedRecordResults(jobId)) {
                    BufferedReader br = new BufferedReader(r);
                    br.lines().forEach(line -> {
                        if (!line.trim().isEmpty()) {
                            log.error("[Salesforce] Failed record for job {}: {}", jobId, line);
                        }
                    });
                } catch (IOException e) {
                    log.warn("[Salesforce] Error reading Failed records for job {}", jobId, e);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Error flushing data to Salesforce Bulk API", e);
        } finally {
            csvBuffer.setLength(0);
            recordCount = 0;
            bufferedBytes = 0;
            lastFlushTime = System.currentTimeMillis();
        }
    }
    // placeholder logger: change to your logger (SLF4J)
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SinkBulkApiV2Task.class);
}