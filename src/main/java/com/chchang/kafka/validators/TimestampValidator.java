package com.chchang.kafka.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.DateTimeException;
import java.time.Instant;

public class TimestampValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        String timestamp = (String) value;
        try {
            Instant.parse(timestamp);
        } catch (DateTimeException e) {
            throw new ConfigException(name, value, "Wasn't able to parse the timestamp, make sure it is formatted according to ISO-8601 standards");
        }
    }
}
