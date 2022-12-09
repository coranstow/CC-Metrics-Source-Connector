package io.confluent;

import org.apache.kafka.common.config.*;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CCloudMetricsSourceConnector extends SourceConnector {


    static final String TASKS_MAX = "tasks.max";

    private Map<String, String> properties;

    private static final Logger log = LoggerFactory.getLogger(CCloudMetricsSourceConnector.class);

    private CCloudMetricsConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        try {
            properties = props;
            config = new CCloudMetricsConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException("Could not start the CCloud Metrics Connector because of a configuration Error");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CCloudMetricsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        List<String> resources = config.getResourceIds();
        int nTasks = Math.min(maxTasks, resources.size());

        List<List<String>> taskResources = new ArrayList<>(nTasks);

        for (int i = 0; i < resources.size(); i++) {
            if (i < nTasks) {
                taskResources.add(i, new ArrayList<>(nTasks));
            }
            taskResources.get(i % nTasks).add(resources.get(i));
        }

        for (int i = 0; i < nTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(properties);
            taskConfig.put(CCloudMetricsSourceTask.TASK_ID, Integer.toString(i));
            taskConfig.remove(CCloudMetricsConnectorConfig.RESOURCE_ID_CONF);
            taskConfig.put(CCloudMetricsConnectorConfig.RESOURCE_ID_CONF, String.join(",",taskResources.get(i)));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CCloudMetricsConnectorConfig.conf();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        try {
            this.config = new CCloudMetricsConnectorConfig(connectorConfigs);
        } catch (ConfigException e) {
            return config;
        }
//
//        for (ConfigValue v : config.configValues()) {
//
//        }

//        try {
//            ConfigValue tasksMax  =  config.configValues().stream().filter(v -> v.name().equals("tasks.max")).findFirst().orElseThrow();
//            if ((int)tasksMax.value() > this.config.getResourceIds().size()) tasksMax.addErrorMessage("Cannot define more tasks than resources for metrics fetch");
//
//        }

        return config;
    }
}
