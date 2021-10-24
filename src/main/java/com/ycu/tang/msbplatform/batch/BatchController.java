package com.ycu.tang.msbplatform.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RestController;

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

}
