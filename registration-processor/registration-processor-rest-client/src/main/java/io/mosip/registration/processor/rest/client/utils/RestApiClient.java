package io.mosip.registration.processor.rest.client.utils;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.core.util.DateUtils;
import io.mosip.kernel.core.util.StringUtils;
import io.mosip.kernel.core.util.TokenHandlerUtil;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.tracing.ContextualData;
import io.mosip.registration.processor.core.tracing.TracingConstant;
import io.mosip.registration.processor.rest.client.audit.dto.Metadata;
import io.mosip.registration.processor.rest.client.audit.dto.SecretKeyRequest;
import io.mosip.registration.processor.rest.client.audit.dto.TokenRequestDTO;
import io.mosip.registration.processor.rest.client.exception.TokenGenerationFailedException;

import jakarta.annotation.PostConstruct;

/**
 * A utility class for making REST API calls with token-based authentication.
 * This class provides methods for performing HTTP GET, POST, PATCH, PUT, HEAD, and DELETE requests
 * using Spring's RestTemplate, with optimized connection pooling and thread-safe token management.
 * It retrieves authentication tokens from a configured token issuer and handles token expiration
 * by resetting the token on 401 errors.
 *
 * @author Rishabh Keshari
 */
@Component
public class RestApiClient {

    /** The logger instance for logging operations and errors. */
    private static final Logger logger = RegProcessorLogger.getLogger(RestApiClient.class);

    /** The RestTemplateBuilder for creating RestTemplate instances. */
    @Autowired
    private RestTemplateBuilder builder;

    /** The environment to retrieve configuration properties. */
    @Autowired
    private Environment environment;

    /** The RestTemplate configured for self-token authentication. */
    @Autowired
    @Qualifier("selfTokenRestTemplate")
    private RestTemplate localRestTemplate;

    /** The ObjectMapper for JSON serialization and deserialization. */
    @Autowired
    private ObjectMapper objMp;

    /** The prefix for the Authorization header. */
    private static final String AUTHORIZATION = "Authorization=";

    /** Thread-safe storage for the bearer token. */
    private static final AtomicReference<String> bearerToken = new AtomicReference<>();

    /** The HTTP client with connection pooling for token requests. */
    private static volatile CloseableHttpClient pooledHttpClient;

    /** The URL of the token issuer. */
    private String tokenIssuerUrl;

    /** The client ID for token requests. */
    private String tokenClientId;

    /** The application ID for token requests. */
    private String tokenAppId;

    /** The secret key for token requests. */
    private String tokenSecretKey;

    /** The ID for the token request DTO. */
    private String tokenId;

    /** The version for the token request DTO. */
    private String tokenVersion;

    /** The endpoint for token API requests. */
    private String tokenApiEndpoint;

    /** The maximum total connections in the HTTP connection pool. */
    private int maxConnTotal;

    /** The maximum connections per route in the HTTP connection pool. */
    private int maxConnPerRoute;

    /** The duration (in seconds) after which idle connections are evicted. */
    private long idleConnEvictSecs;

    /**
     * Initializes the RestApiClient by loading configuration properties and setting up
     * the HTTP client with connection pooling for efficient token requests.
     */
    @PostConstruct
    public void init() {
        tokenIssuerUrl = environment.getProperty("token.request.issuerUrl");
        tokenClientId = environment.getProperty("token.request.clientId");
        tokenAppId = environment.getProperty("token.request.appid");
        tokenSecretKey = environment.getProperty("token.request.secretKey");
        tokenId = environment.getProperty("token.request.id");
        tokenVersion = environment.getProperty("token.request.version");
        tokenApiEndpoint = environment.getProperty("KEYBASEDTOKENAPI");

        maxConnTotal = Integer.parseInt(environment.getProperty("mosip.kernel.httpclient.max.connection", "100"));
        maxConnPerRoute = Integer.parseInt(environment.getProperty("mosip.kernel.httpclient.max.connection.per.route", "20"));
        idleConnEvictSecs = Long.parseLong(environment.getProperty("mosip.kernel.httpclient.idle.evict.seconds", "30"));

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(maxConnTotal);
        connManager.setDefaultMaxPerRoute(maxConnPerRoute);
        pooledHttpClient = HttpClients.custom()
                .setConnectionManager(connManager)
                .evictIdleConnections(TimeValue.ofSeconds(idleConnEvictSecs))
                .setConnectionReuseStrategy((request, response, context) -> true)
                .build();
    }

    /**
     * Performs an HTTP GET request to the specified URI.
     *
     * @param <T>          the type of the response object
     * @param uri          the URI to send the GET request to
     * @param responseType the class of the response object
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T getApi(URI uri, Class<?> responseType) throws Exception {
        try {
            HttpEntity<Object> entity = setRequestHeader(null, null);
            return (T) localRestTemplate.exchange(uri, HttpMethod.GET, entity, responseType).getBody();
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Performs an HTTP POST request to the specified URI.
     *
     * @param <T>           the type of the response object
     * @param uri           the URI to send the POST request to
     * @param mediaType     the media type of the request body (e.g., application/json)
     * @param requestType   the request body object
     * @param responseClass the class of the response object
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T postApi(String uri, MediaType mediaType, Object requestType, Class<?> responseClass) throws Exception {
        try {
            logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
                    LoggerFileConstant.APPLICATIONID.toString(), "POST request to: " + uri);
            return (T) localRestTemplate.postForObject(uri, setRequestHeader(requestType, mediaType), responseClass);
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Performs an HTTP PATCH request to the specified URI with the given media type.
     *
     * @param <T>           the type of the response object
     * @param uri           the URI to send the PATCH request to
     * @param mediaType     the media type of the request body (e.g., application/json)
     * @param requestType   the request body object
     * @param responseClass the class of the response object
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T patchApi(String uri, MediaType mediaType, Object requestType, Class<?> responseClass) throws Exception {
        try {
            logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
                    LoggerFileConstant.APPLICATIONID.toString(), "PATCH request to: " + uri);
            return (T) localRestTemplate.patchForObject(uri, setRequestHeader(requestType, mediaType), responseClass);
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Performs an HTTP PATCH request to the specified URI without a media type.
     *
     * @param <T>           the type of the response object
     * @param uri           the URI to send the PATCH request to
     * @param requestType   the request body object
     * @param responseClass the class of the response object
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    public <T> T patchApi(String uri, Object requestType, Class<?> responseClass) throws Exception {
        return patchApi(uri, null, requestType, responseClass);
    }

    /**
     * Performs an HTTP PUT request to the specified URI.
     *
     * @param <T>           the type of the response object
     * @param uri           the URI to send the PUT request to
     * @param requestType   the request body object
     * @param responseClass the class of the response object
     * @param mediaType     the media type of the request body (e.g., application/json)
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T putApi(String uri, Object requestType, Class<?> responseClass, MediaType mediaType) throws Exception {
        try {
            logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
                    LoggerFileConstant.APPLICATIONID.toString(), "PUT request to: " + uri);
            ResponseEntity<T> response = (ResponseEntity<T>) localRestTemplate.exchange(uri, HttpMethod.PUT,
                    setRequestHeader(requestType, mediaType), responseClass);
            return response.getBody();
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Performs an HTTP HEAD request to the specified URI.
     *
     * @param uri the URI to send the HEAD request to
     * @return the HTTP status code of the response
     * @throws Exception if the request fails or an error occurs
     */
    public int headApi(URI uri) throws Exception {
        try {
            HttpStatusCode httpStatus = localRestTemplate
                    .exchange(uri, HttpMethod.HEAD, setRequestHeader(null, null), Object.class).getStatusCode();
            return httpStatus.value();
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Performs an HTTP DELETE request to the specified URI.
     *
     * @param <T>          the type of the response object
     * @param uri          the URI to send the DELETE request to
     * @param responseType the class of the response object
     * @return the response body deserialized into the specified type
     * @throws Exception if the request fails or an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T deleteApi(URI uri, Class<?> responseType) throws Exception {
        try {
            return (T) localRestTemplate.exchange(uri, HttpMethod.DELETE, setRequestHeader(null, null), responseType)
                    .getBody();
        } catch (Exception e) {
            logError(e);
            tokenExceptionHandler(e);
            throw e;
        }
    }

    /**
     * Retrieves an authentication token from the configured token issuer.
     * If a valid token exists, it is reused; otherwise, a new token is requested.
     *
     * @return the Authorization header value (e.g., "Authorization=Bearer <token>")
     * @throws IOException if an I/O error occurs during the token request
     */
    public String getToken() throws IOException {
        String currentToken = bearerToken.get();
        if (StringUtils.isNotEmpty(currentToken) &&
                TokenHandlerUtil.isValidBearerToken(currentToken, tokenIssuerUrl, tokenClientId)) {
            return AUTHORIZATION + currentToken;
        }

        TokenRequestDTO<SecretKeyRequest> tokenRequestDTO = new TokenRequestDTO<>();
        tokenRequestDTO.setId(tokenId);
        tokenRequestDTO.setMetadata(new Metadata());
        tokenRequestDTO.setRequesttime(DateUtils.getUTCCurrentDateTimeString());
        tokenRequestDTO.setRequest(setSecretKeyRequestDTO());
        tokenRequestDTO.setVersion(tokenVersion);

        HttpPost post = new HttpPost(tokenApiEndpoint);
        try {
            StringEntity postingString = new StringEntity(objMp.writeValueAsString(tokenRequestDTO));
            post.setEntity(postingString);
            post.setHeader("Content-type", "application/json");
            post.setHeader(TracingConstant.TRACE_HEADER,
                    (String) ContextualData.getOrDefault(TracingConstant.TRACE_ID_KEY));

            HttpClientResponseHandler<String> responseHandler = response -> {
                try (ClassicHttpResponse httpResponse = response) {
                    Header[] cookies = httpResponse.getHeaders("Set-Cookie");
                    if (cookies.length == 0) {
                        throw new TokenGenerationFailedException();
                    }
                    String token = cookies[0].getValue();
                    return token.substring(14, token.indexOf(';'));
                }
            };

            String newToken = pooledHttpClient.execute(post, responseHandler);
            bearerToken.compareAndSet(currentToken, newToken);
            return AUTHORIZATION + newToken;
        } catch (IOException e) {
            logError(e);
            throw e;
        }
    }

    /**
     * Creates a SecretKeyRequest DTO for token requests.
     *
     * @return a SecretKeyRequest object with configured app ID, client ID, and secret key
     */
    private SecretKeyRequest setSecretKeyRequestDTO() {
        SecretKeyRequest request = new SecretKeyRequest();
        request.setAppId(tokenAppId);
        request.setClientId(tokenClientId);
        request.setSecretKey(tokenSecretKey);
        return request;
    }

    /**
     * Sets up the HTTP request headers, including the Authorization token and tracing headers.
     *
     * @param requestType the request body object (optional)
     * @param mediaType   the media type of the request body (optional)
     * @return an HttpEntity containing the headers and optional request body
     * @throws IOException if an error occurs while retrieving the token
     */
    @SuppressWarnings("unchecked")
    private HttpEntity<Object> setRequestHeader(Object requestType, MediaType mediaType) throws IOException {
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        headers.add(TracingConstant.TRACE_HEADER, (String) ContextualData.getOrDefault(TracingConstant.TRACE_ID_KEY));
        headers.add("Authorization", getToken());
        if (mediaType != null) {
            headers.add("Content-Type", mediaType.toString());
        }

        if (requestType != null) {
            try {
                HttpEntity<Object> httpEntity = (HttpEntity<Object>) requestType;
                HttpHeaders httpHeader = httpEntity.getHeaders();
                for (String key : httpHeader.keySet()) {
                    List<String> values = httpHeader.get(key);
                    if (values != null && !values.isEmpty() &&
                            !(headers.containsKey("Content-Type") && key.equalsIgnoreCase("Content-Type"))) {
                        headers.add(key, values.get(0));
                    }
                }
                return new HttpEntity<>(httpEntity.getBody(), headers);
            } catch (ClassCastException e) {
                return new HttpEntity<>(requestType, headers);
            }
        }
        return new HttpEntity<>(headers);
    }

    /**
     * Handles exceptions related to token authentication, resetting the token on HTTP 401 errors.
     *
     * @param e the exception to handle
     */
    public void tokenExceptionHandler(Exception e) {
        if (e instanceof HttpStatusCodeException ex && ex.getStatusCode().value() == 401) {
            logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
                    LoggerFileConstant.APPLICATIONID.toString(), "Authentication failed. Resetting auth token.");
            bearerToken.set(null);
        }
    }

    /**
     * Logs an error with session and application IDs, including the exception stack trace.
     *
     * @param e the exception to log
     */
    private void logError(Exception e) {
        logger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
                LoggerFileConstant.APPLICATIONID.toString(), e.getMessage() + ExceptionUtils.getStackTrace(e));
    }

    /**
     * Returns the configured RestTemplate instance.
     *
     * @return the RestTemplate used for API calls
     */
    public RestTemplate getRestTemplate() {
        return localRestTemplate;
    }
}