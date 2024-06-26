package com.chchang.kafka;

import com.chchang.kafka.validators.BatchSizeValidator;
import com.chchang.kafka.validators.TimestampValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;


public class GithubSourceConnectorConfig extends AbstractConfig {
  public static final String TOPIC_CONFIG = "topic";
  public static final String OWNER_CONFIG = "github.owner";
  public static final String REPO_CONFIG = "github.repo";
  public static final String SINCE_CONFIG = "since.timestamp";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  public static final String AUTH_USERNAME_CONFIG = "auth.username";
  public static final String AUTH_PASSWORD_CONFIG = "auth.password";

  private static final String TOPIC_DOC = "Topic to write to";
  private static final String OWNER_DOC = "Owner of the repository you'd like to follow";
  private static final String REPO_DOC = "Repository you'd like to follow";
  private static final String SINCE_DOC = "Only issues updated at or after this time are returned.\nThis is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ.\nDefaults to a year from first launch.";
  private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";
  private static final String AUTH_USERNAME_DOC = "Optional Username to authenticate calls";
  private static final String AUTH_PASSWORD_DOC = "Optional Password to authenticate calls";

  private static final int BATCH_SIZE_DEFAULT = 100;
  private static final String SINCE_DEFAULT = ZonedDateTime.now().minusYears(1).toInstant().toString();
  private static final String AUTH_USERNAME_DEFAULT = "";
  private static final String AUTH_PASSWORD_DEFAULT = "";


  public GithubSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public GithubSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
            .define(OWNER_CONFIG, Type.STRING, Importance.HIGH, OWNER_DOC)
            .define(REPO_CONFIG, Type.STRING, Importance.HIGH, REPO_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC)
            .define(SINCE_CONFIG, Type.STRING, SINCE_DEFAULT, new TimestampValidator(), Importance.HIGH, SINCE_DOC)
            .define(AUTH_USERNAME_CONFIG, Type.STRING, AUTH_USERNAME_DEFAULT, Importance.HIGH, AUTH_USERNAME_DOC)
            .define(AUTH_PASSWORD_CONFIG, Type.PASSWORD, AUTH_PASSWORD_DEFAULT, Importance.HIGH, AUTH_PASSWORD_DOC);
  }

  public String getOwnerConfig() { return this.getString(OWNER_CONFIG); }

  public String getRepoConfig() { return this.getString(REPO_CONFIG); }

  public Integer getBatchSizeConfig() { return this.getInt(BATCH_SIZE_CONFIG); }

  public Instant getSinceConfig() { return Instant.parse(this.getString(SINCE_CONFIG)); }

  public String getTopicConfig() { return this.getString(TOPIC_CONFIG); }

  public String getAuthUsernameConfig() { return this.getString(AUTH_USERNAME_CONFIG); }

  public String getAuthPasswordConfig() { return this.getString(AUTH_PASSWORD_CONFIG); }
}
