package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.nio.ByteBuffer;

public class ToSerializedLong
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    long val = call.getArguments().getLong(0);
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(val);
    call.getOutputCollector().add(
            new Tuple(buffer.array()));
  }
}
