package com.chchang.kafka.apis;

import com.chchang.kafka.GithubSourceConnectorConfig;
import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class GithubAPIHttpClient {
    private static final Logger log = LoggerFactory.getLogger(GithubAPIHttpClient.class);

    private Integer XRateLimit = 9999;
    private Integer XRateRemaining = 9999;
    private long XRateReset = Instant.MAX.getEpochSecond();

    GithubSourceConnectorConfig config;

    public static final String X_RATE_LIMIT_LIMIT_HEADER="X-RateLimit-Limit";
    public static final String X_RATE_LIMIT_REMAINING_HEADER="X-RateLimit-Remaining";
    public static final String X_RATE_LIMIT_RESET_HEADER="X-RateLimit-Reset";

    public GithubAPIHttpClient(GithubSourceConnectorConfig config){
        this.config = config;
    }

    public JSONArray getNextIssues(Integer page, Instant since) throws InterruptedException {
        HttpResponse<JsonNode> jsonResponse;
        try {
            jsonResponse = getNextIssuesAPI(page, since);

            Headers headers = jsonResponse.getHeaders();
            XRateLimit = Integer.valueOf(headers.getFirst(X_RATE_LIMIT_LIMIT_HEADER));
            XRateRemaining = Integer.valueOf(headers.getFirst(X_RATE_LIMIT_REMAINING_HEADER));
            XRateReset = Integer.valueOf(headers.getFirst(X_RATE_LIMIT_RESET_HEADER));
            switch (jsonResponse.getStatus()){
                case 200:
                    return jsonResponse.getBody().getArray();
                case 401:
                    throw new ConnectException("Bad GitHub credentials provided, please edit your config");
                case 403:
                    log.info(jsonResponse.getBody().getObject().getString("message"));
                    log.info(String.format("Your rate limit is %s", XRateLimit));
                    log.info(String.format("Your remaining calls is %s", XRateRemaining));
                    log.info(String.format("The limit will reset at %s",
                            LocalDateTime.ofInstant(Instant.ofEpochSecond(XRateReset), ZoneOffset.systemDefault())));
                    long sleepTime = XRateReset - Instant.now().getEpochSecond();
                    log.info(String.format("Sleeping for %s seconds", sleepTime ));
                    Thread.sleep(1000 * sleepTime);
                    return getNextIssues(page, since);
                default:
                    log.error(constructUrl(page, since));
                    log.error(String.valueOf(jsonResponse.getStatus()));
                    log.error(jsonResponse.getBody().toString());
                    log.error(jsonResponse.getHeaders().toString());
                    log.error("Unknown error: Sleeping 5 seconds " +
                            "before re-trying");
                    Thread.sleep(5000L);
                    return getNextIssues(page, since);
            }
        } catch (UnirestException e) {
            e.printStackTrace();
            Thread.sleep(5000L);
            return new JSONArray();
        }
    }

    public void sleep() throws InterruptedException {
        long sleepTime = (long) Math.ceil(
                (double) (XRateReset - Instant.now().getEpochSecond()) / XRateRemaining);
        log.debug(String.format("Sleeping for %s seconds", sleepTime ));
        Thread.sleep(1000 * sleepTime);
    }

    public void sleepIfNeed() throws InterruptedException {
        // Sleep if needed
        if (XRateRemaining <= 10 && XRateRemaining > 0) {
            log.info(String.format("Approaching limit soon, you have %s requests left", XRateRemaining));
            sleep();
        }
    }

    private HttpResponse<JsonNode> getNextIssuesAPI(Integer page, Instant since) throws UnirestException {
        GetRequest unirest = Unirest.get(constructUrl(page, since));
        if (!config.getAuthUsernameConfig().isEmpty() && !config.getAuthPasswordConfig().isEmpty() ){
            unirest = unirest.basicAuth(config.getAuthUsernameConfig(), config.getAuthPasswordConfig());
        }
        log.debug(String.format("GET %s", unirest.getUrl()));
        return unirest.asJson();
    }

    private String constructUrl(Integer page, Instant since){
        return String.format(
                "https://api.github.com/repos/%s/%s/issues?page=%s&per_page=%s&since=%s&state=all&direction=asc&sort=updated",
                config.getOwnerConfig(),
                config.getRepoConfig(),
                page,
                config.getBatchSizeConfig(),
                since.toString());
    }
}
