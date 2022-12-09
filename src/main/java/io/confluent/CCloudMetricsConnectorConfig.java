package io.confluent;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;


import java.util.List;
import java.util.Map;


public class CCloudMetricsConnectorConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG_CONF = "topic";
    public static final String TOPIC_CONFIG_DOC = "The topic to publish data to.";
    public static final String POLL_INTERVAL_CONF = "poll.interval";
    public static final String POLL_INTERVAL_DOC = "poll.interval";
    public static final long POLL_INTERVAL_DEFAULT_VALUE = 60000;
    public static final String RESOURCE_ID_CONF = "resource.kafka.id";
    public static final String RESOURCE_ID_DOC = "resource.kafka.id";
    public static final String API_KEY_CONF = "ccloud.api.key";
    public static final String API_KEY_DOC = "ccloud.api.key";
    public static final String API_SECRET_CONF = "ccloud.api.secret";
    public static final String API_SECRET_DOC = "ccloud.api.secret";
    static final String RESOURCE_PREFIX = "lkc";

    public CCloudMetricsConnectorConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }

    public CCloudMetricsConnectorConfig(Map<String, String> originals) {
        this(conf(), originals);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(POLL_INTERVAL_CONF, ConfigDef.Type.LONG, POLL_INTERVAL_DEFAULT_VALUE, ConfigDef.Importance.HIGH, POLL_INTERVAL_DOC)
                .define(TOPIC_CONFIG_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_CONFIG_DOC)
                .define(RESOURCE_ID_CONF, ConfigDef.Type.LIST, "", new ResourceValidator(), ConfigDef.Importance.HIGH, RESOURCE_ID_DOC)
                .define(API_KEY_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, API_KEY_DOC)
                .define(API_SECRET_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, API_SECRET_DOC);
    }

    public String getKafkaTopic() {
        return this.getString(TOPIC_CONFIG_CONF);
    }
    public Long getPollInterval() {
        return this.getLong(POLL_INTERVAL_CONF);
    }
    public List<String> getResourceIds() {
        return this.getList(RESOURCE_ID_CONF);
    }
    public String getApiKey() {
        return this.getString(API_KEY_CONF);
    }
    public Password getApiSecret() {
        return this.getPassword(API_SECRET_CONF);
    }
    private static class ResourceValidator implements Validator {

        @Override
        public void ensureValid(String name, Object value) {
            @SuppressWarnings("unchecked") // This is the callback for the list of resources.
            List<String> values = (List<String>) value;
            for (String resource: values) {
                if (!resource.startsWith(RESOURCE_PREFIX))
                    throw new ConfigException("Only Cloud Cluster resources starting with \"lkc-\" are supported at this time.");
            }
        }
    }
}

