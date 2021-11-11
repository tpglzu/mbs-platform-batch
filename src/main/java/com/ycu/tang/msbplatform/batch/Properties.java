package com.ycu.tang.msbplatform.batch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class Properties {

  @Value("${hadoop.namenode.url}")
  private String namenodeUrl;

  @Value("${hadoop.pail.path.root}")
  private String root;

  @Value("${hadoop.pail.path.data.root}")
  private String dataRoot;

  @Value("${hadoop.pail.path.outputs.root}")
  private String outputsRoot;

  @Value("${hadoop.pail.path.master.root}")
  private String masterRoot;

  @Value("${hadoop.pail.path.new.root}")
  private String newRoot;

  @Value("${mongodb.url}")
  private String dbUrl;
  @Value("${mongodb.port}")
  private Integer dbPort;
  @Value("${mongodb.database}")
  private String dbName;
  @Value("${mongodb.database.speed}")
  private String dbNameSpeed;

  public String getNamenodeUrl() {
    return namenodeUrl;
  }

  public String getRoot() {
    return root;
  }

  public String getDataRoot() {
    return dataRoot;
  }

  public String getOutputsRoot() {
    return outputsRoot;
  }

  public String getMasterRoot() {
    return masterRoot;
  }

  public String getNewRoot() {
    return newRoot;
  }

  public String getDbUrl() {
    return dbUrl;
  }

  public Integer getDbPort() {
    return dbPort;
  }

  public String getDbName() {
    return dbName;
  }

  public String getDbNameSpeed() {
    return dbNameSpeed;
  }
}
