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

    private String consumerKey;

    private String consumerSecret;

    private String username;

    private String password;

    private String apiVersion;

    private Supplier<AccessToken> accessTokenSupplier;

    public Bulk2ClientBuilder withPasswordAndTokenEndpoint(String tokenEndpoint, String consumerKey, String consumerSecret, String username, String password) {
        this.tokenRequestEndpoint = tokenEndpoint;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.username =username;
        this.password = password;
        this.accessTokenSupplier = () -> this.getAccessTokenUsingPassword(tokenEndpoint, consumerKey, consumerSecret, username, password);

        return this;
    }

    public Bulk2ClientBuilder withApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public Bulk2Client build()
            throws IOException {
        AccessToken token = accessTokenSupplier.get();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(authorizationInterceptor(accessTokenSupplier,tokenRequestEndpoint, consumerKey, consumerSecret, username, password))
                .addInterceptor(httpLoggingInterceptor(HttpLoggingInterceptor.Level.BODY))
                .build();
        return new Bulk2Client(new RestRequester(client), token.getInstanceUrl(), apiVersion);
    }

    private AccessToken getAccessTokenUsingPassword(String endpoint, String consumerKey, String consumerSecret, String username, String password) {
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

    private HttpLoggingInterceptor httpLoggingInterceptor(HttpLoggingInterceptor.Level level) {
        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(message -> log.info(message));
        logging.setLevel(level);

        return logging;
    }

    private Interceptor authorizationInterceptor(Supplier<AccessToken> tokenSupplier, String endpoint, String consumerKey, String consumerSecret, String username, String password) {
        return chain -> {
            Request originalRequest = chain.request();

            // initial request
            AccessToken currentToken = tokenSupplier.get();
            Request requestWithAuth = originalRequest.newBuilder()
                    .addHeader("Authorization", "Bearer " + currentToken.getAccessToken())
                    .build();

            Response response = chain.proceed(requestWithAuth);

            if (response.code() == 401) { // token expired
                log.warn("Salesforce token expired, attempting to refresh...");

                synchronized (this) {
                    // force refresh
                    AccessToken newToken = getAccessTokenUsingPassword(
                            endpoint,
                            consumerKey,
                            consumerSecret,
                            username,
                            password
                    );

                    // replace the supplier content if needed
                    this.accessTokenSupplier = () -> newToken;

                    // retry the request with the new token
                    Request retryRequest = originalRequest.newBuilder()
                            .removeHeader("Authorization")
                            .addHeader("Authorization", "Bearer " + newToken.getAccessToken())
                            .build();

                    response.close(); // close old response before retry
                    return chain.proceed(retryRequest);
                }
            }

            return response;
        };
    }
}