package com.jnditifei.salesforces.bulkv2;

import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class Bulk2ClientBuilder {

    private static final Logger log = LoggerFactory.getLogger(Bulk2ClientBuilder.class);

    private String tokenRequestEndpoint;

    private Supplier<AccessToken> accessTokenSupplier;

    public Bulk2ClientBuilder withPasswordAndTokenEndpoint(String tokenEndpoint, String consumerKey, String consumerSecret, String username, String password) {
        this.tokenRequestEndpoint = tokenEndpoint;
        this.accessTokenSupplier = () -> this.getAccessTokenUsingPassword(consumerKey, consumerSecret, username, password);

        return this;
    }

    public Bulk2ClientBuilder withSessionId(String token, String instanceUrl) {
        this.accessTokenSupplier = () -> {
            AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken(token);
            accessToken.setInstanceUrl(instanceUrl);
            return accessToken;
        };

        return this;
    }

    public Bulk2Client build()
            throws IOException {
        AccessToken token = accessTokenSupplier.get();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(authorizationInterceptor(token.getAccessToken()))
                .addInterceptor(httpLoggingInterceptor(HttpLoggingInterceptor.Level.BODY))
                .build();
        return new Bulk2Client(new RestRequester(client), token.getInstanceUrl());
    }

    private AccessToken getAccessTokenUsingPassword(String consumerKey, String consumerSecret, String username, String password) {
        String endpoint = tokenRequestEndpoint;
        HttpUrl authorizeUrl = HttpUrl.parse(endpoint).newBuilder().build();

        RequestBody requestBody = new FormBody.Builder()
                .add("grant_type", "password")
                .add("client_id", consumerKey)
                .add("client_secret", consumerSecret)
                .add("username", username)
                .add("password", password)
                .build();

        Request request = new Request.Builder()
                .url(authorizeUrl)
                .post(requestBody)
                .build();

        OkHttpClient client = new OkHttpClient().newBuilder()
                // .addInterceptor(new SigningInterceptor(consumer))
                .addInterceptor(httpLoggingInterceptor(HttpLoggingInterceptor.Level.BASIC))
                .build();

        try {
            Response response = client.newCall(request).execute();
            ResponseBody responseBody = response.body();

            return Json.decode(responseBody.string(), AccessToken.class);
        } catch (IOException e) {
            throw new BulkRequestException(e);
        }
    }

    private Interceptor authorizationInterceptor(String token) {
        return chain -> {
            Request request = chain.request().newBuilder()
                    .addHeader("Authorization", "Bearer " + token)
                    .build();
            return chain.proceed(request);
        };
    }

    private HttpLoggingInterceptor httpLoggingInterceptor(HttpLoggingInterceptor.Level level) {
        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(message -> log.info(message));
        logging.setLevel(level);

        return logging;
    }
}
