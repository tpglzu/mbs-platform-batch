package com.ycu.tang.msbplatform.batch;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RestController;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@RestController
public class BatchController {
  Logger logger = LoggerFactory.getLogger(BatchController.class);

  @Autowired
  protected BatchWorkflow workflow;

  @Scheduled(fixedDelayString = "${job.batch.interval}")
  public void job() {
    try {
      logger.info("job executed start ...");
      workflow.run();
    }catch (Exception e){
      logger.error("job executed failed ...", e);

    }

  }

  public static MongoURI getMongoURI(Configuration var0, String var1) {
    String var2 = var0.get(var1);
    return var2 != null && !var2.trim().isEmpty() ? new MongoURI(var2) : null;
  }

  private static final Mongo.Holder _mongos = new Mongo.Holder();
}
