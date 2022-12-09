package io.confluent;


import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CCloudMetricsSourceTask extends SourceTask {

    public static final String TASK_ID = "task.id";
    private static final String CCLOUD_METRICS_ENDPOINT ="https://api.telemetry.confluent.cloud/v2/metrics/cloud/export";
    private static final Logger log = LoggerFactory.getLogger(CCloudMetricsSourceTask.class);

    private String topic;
    private Long interval;
    private String authString;
    private URI uri;


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        CCloudMetricsConnectorConfig config = new CCloudMetricsConnectorConfig(props);

        topic = config.getKafkaTopic();
        interval = config.getPollInterval();
        List<String> resources = config.getResourceIds();
        String apikey = config.getApiKey();

        authString = Base64.getEncoder().encodeToString((apikey + ":" + config.getApiSecret().value()).getBytes());

        log.info("Starting CCloud Metrics Task");



        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String resource : resources) {
            if (first) {
                first = false;
            } else {
                sb.append("&");
            }
            sb.append(CCloudMetricsConnectorConfig.RESOURCE_ID_CONF);
            sb.append("=");
            sb.append(resource);
        }
        String requestParameters = sb.toString();

        try {
            uri = new URI(CCLOUD_METRICS_ENDPOINT + "?" + requestParameters);

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            log.debug("CCloudMetricsConnector task sleep for " + interval);
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            return null;
        }
        LinkedList<SourceRecord> returnList = new LinkedList<>();
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .uri(uri)
                    .headers("Authorization", "Basic " + authString)
                    .GET()
                    .build();
            log.debug("Executing HTTP Request to " + uri.toString() + "\"");
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (response.statusCode() == 200) {
                @SuppressWarnings("unchecked") //Empty maps
                SourceRecord returnRecord = new SourceRecord(Collections.EMPTY_MAP, Collections.EMPTY_MAP
                        , topic, null, Schema.STRING_SCHEMA, response.body());

                returnList.add(returnRecord);
            } else {
                log.warn("Confluent Cloud Metrics API responded with HTTP Code: " + response.statusCode());
            }
        } catch (IOException e) {
            log.warn("CCloudMetricsConnector encountered IOException: " + e.getMessage());
        }
        return returnList;
    }

    @Override
    public void stop() {

    }

}
