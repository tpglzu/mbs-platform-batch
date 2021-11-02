package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.nio.ByteBuffer;

public class ToBounceRate
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    long l1 = call.getArguments().getLong(0);
    long l2 = call.getArguments().getLong(1);
    long rate = l1 / l2;
    call.getOutputCollector().add(new Tuple(rate));
  }
}
