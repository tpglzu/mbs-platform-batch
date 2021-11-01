package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class Debug extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    System.out.println("DEBUG: " + call.getArguments().toString());
    call.getOutputCollector().add(new Tuple(1));
  }
}
