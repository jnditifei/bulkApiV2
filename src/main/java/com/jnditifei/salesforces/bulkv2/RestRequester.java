package com.jnditifei.salesforces.bulkv2;

import com.fasterxml.jackson.core.type.TypeReference;

import com.jnditifei.salesforces.bulkv2.request.CreateJobRequest;
import com.jnditifei.salesforces.bulkv2.response.ErrorResponse;
import okhttp3.*;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RestRequester {

    private static final Logger log = LoggerFactory.getLogger(RestRequester.class);

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    private static final MediaType CSV_MEDIA_TYPE = MediaType.parse("text/csv");

    private static final TypeReference<List<ErrorResponse>> ERRORS_TYPE_REFERENCE = new TypeReference<List<ErrorResponse>>() {

    };

    private final OkHttpClient client;

    public RestRequester(OkHttpClient client) {
        this.client = client;
    }

    public <T> T putCsv(String url, String requestData, Class<T> responseClass) {
        RequestBody requestBody = (requestData == null) ? null : RequestBody.create(ByteString.encodeUtf8(requestData), CSV_MEDIA_TYPE);

        return request(url, "PUT", new HashMap<>(), requestBody, responseClass);
    }

    public Reader getCsv(String url) {
        return request(url, "GET", new HashMap<>(), null, Reader.class);
    }

    public <T> T get(String url, Class<T> responseClass) {
        return request(url, "GET", new HashMap<>(), null, responseClass);
    }

    public <T> T get(String url, Map<String, String> queryParams, Class<T> responseClass) {
        return request(url, "GET", queryParams, null, responseClass);
    }

    public <T> T post(String url, Object requestData, Class<T> responseClass) {
        Object transformedRequest = requestData;

        // TODO: avoid if-statement due to specific request instance
        if (requestData instanceof CreateJobRequest createJob) {

            RequestBody content = createJob.getContent() != null ? RequestBody.create(createJob.getContent(), CSV_MEDIA_TYPE)
                    : createJob.getContentFile() != null ? RequestBody.create(createJob.getContentFile(), CSV_MEDIA_TYPE)
                    : null;
            if (content != null) {
                transformedRequest = new MultipartBody.Builder()
                        .setType(MultipartBody.FORM)
                        .addFormDataPart("job", null,
                                RequestBody.create(Json.encode(requestData), JSON_MEDIA_TYPE))
                        .addFormDataPart("content", "content", content)
                        .build();
            }
        }

        return requestJson(url, "POST", new HashMap<>(), transformedRequest, responseClass);
    }

    public <T> T put(String url, Object requestData, Class<T> responseClass) {
        return requestJson(url, "PUT", new HashMap<>(), requestData, responseClass);
    }

    public <T> T delete(String url, Object requestData, Class<T> responseClass) {
        return requestJson(url, "DELETE", new HashMap<>(), requestData, responseClass);
    }

    public <T> T patch(String url, Object requestData, Class<T> responseClass) {
        return requestJson(url, "PATCH", new HashMap<>(), requestData, responseClass);
    }

    private <T> T requestJson(String url, String httpMethod, Map<String, String> queryParams, Object requestData, Class<T> responseClass) {
        RequestBody requestBody = (requestData == null) ? null
                : (requestData instanceof RequestBody) ? (RequestBody) requestData
                : RequestBody.create(Json.encode(requestData), JSON_MEDIA_TYPE);

        return request(url, httpMethod, queryParams, requestBody, responseClass);
    }

    private Response doRequest(Request request) {
        try {
            return client.newCall(request).execute();
        } catch (IOException e) {
            throw new BulkRequestException(e);
        }
    }

    private ResponseBody unwrap(Response response) {
        ResponseBody responseBody = response.body();
        if (responseBody == null) {
            throw new BulkRequestException("response body is null.");
        }
        return responseBody;
    }

    private <T> T request(String url, String httpMethod, Map<String, String> queryParams, RequestBody requestBody, Class<T> responseClass) {
        HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();
        queryParams.forEach(urlBuilder::addQueryParameter);

        Request request = new Request.Builder()
                .url(urlBuilder.build())
                .method(httpMethod, requestBody)
                .build();

        if (responseClass.isAssignableFrom(Reader.class)) {
            ResponseBody responseBody = unwrap(doRequest(request));
            return (T) responseBody.charStream(); // close Response/Stream in caller
        }

        try (Response response = doRequest(request)) {
            ResponseBody responseBody = unwrap(response);

            String body = null;
            try {
                body = responseBody.string();
                responseBody.close();
            } catch (IOException e) {
                log.warn(e.getMessage());
            }

            if (response.isSuccessful()) {
                return (body == null || body.isEmpty()) ? null : Json.decode(body, responseClass);
            } else {
                try {
                    List<ErrorResponse> errors = Json.decode(body, ERRORS_TYPE_REFERENCE);
                    throw new BulkRequestException(errors);
                } catch (UncheckedIOException e) {
                    throw new BulkRequestException("Unknown error: " + body);
                }
            }
        }
    }
}
