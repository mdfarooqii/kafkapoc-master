package com.ubs.risk.arisk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AriskFolderPollerConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(AriskFolderPollerConnector.class);
  private AriskFolderPollerConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new AriskFolderPollerConnectorConfig(map);

    //TODO: Add things you need to do to setup your connector.
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return AriskFolderPollerTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    //TODO: Define the individual task configurations that will be executed.
    ArrayList<Map<String, String>> configs = new ArrayList<>(1);
    configs.add(config.originalsStrings());
    return configs;
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return AriskFolderPollerConnectorConfig.conf();
  }
}
