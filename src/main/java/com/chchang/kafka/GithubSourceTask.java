package com.chchang.kafka;

import com.chchang.kafka.apis.GithubAPIHttpClient;
import com.chchang.kafka.models.Issue;
import com.chchang.kafka.models.PullRequest;
import com.chchang.kafka.models.User;
import com.chchang.kafka.utils.DateUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.chchang.kafka.GithubSchemas.*;

public class GithubSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(GithubSourceTask.class);

  private GithubSourceConnectorConfig config;
  private GithubAPIHttpClient githubAPIHttpClient;

  private Instant lastUpdatedAt;
  private Integer lastIssueNumber;

  private Instant nextQuerySince;
  private Integer nextPageToVisit = 1;

  private Map<String, Object> lastSourceOffset;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new GithubSourceConnectorConfig(map);
    githubAPIHttpClient = new GithubAPIHttpClient(config);

    if (lastSourceOffset == null) {
      nextQuerySince = config.getSinceConfig();
      lastIssueNumber = -1;
    } else {
      String jsonOffsetData = new Gson().toJson(lastSourceOffset);
      TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
      Map<String, Object> offsetData = new Gson().fromJson(jsonOffsetData, typeToken.getType());

      Object updatedAt = offsetData.get(UPDATED_AT_FIELD);
      Object issueNumber = offsetData.get(NUMBER_FIELD);
      Object nextPage = offsetData.get(NEXT_PAGE_FIELD);

      if(updatedAt instanceof String){
        nextQuerySince = Instant.parse((String) updatedAt);
      }

      if(issueNumber instanceof String){
        lastIssueNumber = Integer.valueOf((String) issueNumber);
      }

      if (nextPage instanceof String){
        nextPageToVisit = Integer.valueOf((String) nextPage);
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    githubAPIHttpClient.sleepIfNeed();

    JSONArray jsonIssues = githubAPIHttpClient.getNextIssues(nextPageToVisit, nextQuerySince);

    ArrayList<SourceRecord> records = new ArrayList<>();
    for (Object jsonIssue : jsonIssues) {
      String topic = config.getTopicConfig();

      Issue issue = Issue.fromJson((JSONObject) jsonIssue);

      Instant updateAt = issue.getUpdatedAt();
      long updateAtEpochMilli = updateAt.toEpochMilli();

      Struct recordKey = buildRecordKey(issue);
      Struct recordValue = buildRecordValue(issue);

      Map<String, String> sourcePartition = getSourcePartition();
      Map<String, String> sourceOffset = getSourceOffset(updateAt);

      SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, topic, null, KEY_SCHEMA, recordKey, VALUE_SCHEMA, recordValue, updateAtEpochMilli);

      records.add(record);
    }

    if (!records.isEmpty()) {
      log.info(String.format("Fetched %s record(s)", records.size()));
    }

    if (records.size() == 100){
      // we have reached a full batch, we need to get the next one
      nextPageToVisit += 1;
    }
    else {
      nextQuerySince = lastUpdatedAt.plusSeconds(1);
      nextPageToVisit = 1;
      githubAPIHttpClient.sleep();
    }

    return records;
  }

  @Override
  public void stop() {
  }

  private Map<String, String> getSourcePartition() {
    Map<String, String> sourcePartition = new HashMap<>();
    sourcePartition.put(OWNER_FIELD, config.getOwnerConfig());
    sourcePartition.put(REPOSITORY_FIELD, config.getRepoConfig());
    return sourcePartition;
  }

  private Map<String, String> getSourceOffset(Instant updatedAt) {
    Map<String, String> sourceOffset = new HashMap<>();
    sourceOffset.put(UPDATED_AT_FIELD, DateUtils.MaxInstant(updatedAt, nextQuerySince).toString());
    sourceOffset.put(NEXT_PAGE_FIELD, nextPageToVisit.toString());
    return sourceOffset;
  }

  private Struct buildRecordKey(Issue issue){
    Struct key = new Struct(KEY_SCHEMA)
            .put(OWNER_FIELD, config.getOwnerConfig())
            .put(REPOSITORY_FIELD, config.getRepoConfig())
            .put(NUMBER_FIELD, issue.getNumber());

    return key;
  }

  public Struct buildRecordValue(Issue issue){
    Struct valueStruct = new Struct(VALUE_SCHEMA)
            .put(URL_FIELD, issue.getUrl())
            .put(TITLE_FIELD, issue.getTitle())
            .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
            .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
            .put(NUMBER_FIELD, issue.getNumber())
            .put(STATE_FIELD, issue.getState());

    User user = issue.getUser();
    Struct userStruct = new Struct(USER_SCHEMA)
            .put(USER_URL_FIELD, user.getUrl())
            .put(USER_ID_FIELD, user.getId())
            .put(USER_LOGIN_FIELD, user.getLogin());
    valueStruct.put(USER_FIELD, userStruct);

    PullRequest pullRequest = issue.getPullRequest();
    if (pullRequest != null) {
      Struct prStruct = new Struct(PR_SCHEMA)
              .put(PR_URL_FIELD, pullRequest.getUrl())
              .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
      valueStruct.put(PR_FIELD, prStruct);
    }

    return valueStruct;
  }
}