package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.nio.ByteBuffer;

public class ToSerializedLongPair
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    long l1 = call.getArguments().getLong(0);
    long l2 = call.getArguments().getLong(1);
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(l1);
    buffer.putLong(l2);
    call.getOutputCollector().add(new Tuple(buffer.array()));
  }
}
