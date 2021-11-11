package com.ycu.tang.msbplatform.batch;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ViewManager {
  Logger logger = LoggerFactory.getLogger(ViewManager.class);
  @Autowired
  private Properties properties;
  private final List<String> collections = Arrays.asList("page-view", "unique-view", "bounce-view");
  private int currentSpeedViewNo = 1;

  public ViewManager() {
  }

  public void replaceBatchView() {
    this.logger.info("replace batch views. ");
    this.logger.info("original to old. ");
    Iterator var1 = this.collections.iterator();

    String collection;
    String mongoUri;
    DBCollection dbCollection;
    while(var1.hasNext()) {
      collection = (String)var1.next();

      try {
        mongoUri = String.format("mongodb://%s:%d/%s.%s", this.properties.getDbUrl(), this.properties.getDbPort(), this.properties.getDbName(), collection);
        dbCollection = MongoConfigUtil.getCollection(new MongoURI(mongoUri));
        dbCollection.rename(collection + "_old");
      } catch (Exception var7) {
        this.logger.error("rename original to old failed. ", var7);
      }
    }

    this.logger.info("new to original. ");
    var1 = this.collections.iterator();

    while(var1.hasNext()) {
      collection = (String)var1.next();

      try {
        mongoUri = String.format("mongodb://%s:%d/%s.%s", this.properties.getDbUrl(), this.properties.getDbPort(), this.properties.getDbName(), collection + "_new");
        dbCollection = MongoConfigUtil.getCollection(new MongoURI(mongoUri));
        dbCollection.rename(collection);
      } catch (Exception var6) {
        this.logger.error("rename new to original failed. ", var6);
      }
    }

    this.logger.info("drop old. ");
    var1 = this.collections.iterator();

    while(var1.hasNext()) {
      collection = (String)var1.next();

      try {
        mongoUri = String.format("mongodb://%s:%d/%s.%s", this.properties.getDbUrl(), this.properties.getDbPort(), this.properties.getDbName(), collection + "_old");
        dbCollection = MongoConfigUtil.getCollection(new MongoURI(mongoUri));
        dbCollection.drop();
      } catch (Exception var5) {
        this.logger.error("drop old failed. ", var5);
      }
    }

  }

  public void updateSpeedViews() {
    this.logger.info("update speed views. ");
    this.logger.info("clear speed view with no. " + this.currentSpeedViewNo);
    Iterator var1 = this.collections.iterator();

    while(var1.hasNext()) {
      String collection = (String)var1.next();

      try {
        String mongoUri = String.format("mongodb://%s:%d/%s.%s", this.properties.getDbUrl(), this.properties.getDbPort(), this.properties.getDbNameSpeed(), collection + "_" + this.currentSpeedViewNo);
        DBCollection dbCollection = MongoConfigUtil.getCollection(new MongoURI(mongoUri));
        dbCollection.remove(new BasicDBObject());
      } catch (Exception var5) {
        this.logger.error("clear speed view failed. ", var5);
      }
    }

    this.currentSpeedViewNo = Math.abs(this.currentSpeedViewNo - 1);
  }
}