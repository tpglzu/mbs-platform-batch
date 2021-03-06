package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;

import java.util.Iterator;

public class AnalyzeVisits extends CascalogBuffer {
  private static final int VISIT_LENGTH_SECS = 60 * 15;

  public void operate(FlowProcess process, BufferCall call) {
    Iterator<TupleEntry> it = call.getArgumentsIterator();
    int bounces = 0;
    int visits = 0;
    Integer lastTime = null;
    int numInCurrVisit = 0;
    while (it.hasNext()) {
      TupleEntry tuple = it.next();
      int timeSecs = tuple.getInteger(0);
      if (lastTime == null ||
              (timeSecs - lastTime) > VISIT_LENGTH_SECS) {
        visits++;
        if (numInCurrVisit == 1) {
          bounces++;
        }
        numInCurrVisit = 0;
      }
      numInCurrVisit++;
    }
    if (numInCurrVisit == 1) {
      bounces++;
    }
    call.getOutputCollector().add(new Tuple(visits, bounces));
  }
}
