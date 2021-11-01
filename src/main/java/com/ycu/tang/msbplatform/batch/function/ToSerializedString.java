package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.io.UnsupportedEncodingException;

public class ToSerializedString
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    String str = call.getArguments().getString(0);

    try {
      call.getOutputCollector().add(
              new Tuple(str.getBytes("UTF-8")));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
