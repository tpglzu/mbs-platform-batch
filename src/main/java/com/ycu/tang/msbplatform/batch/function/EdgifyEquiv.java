package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import com.ycu.tang.msbplatform.batch.thrift.Data;
import com.ycu.tang.msbplatform.batch.thrift.EquivEdge;

public class EdgifyEquiv extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    Data data = (Data) call.getArguments().getObject(0);
    EquivEdge equiv = data.getDataunit().getEquiv();
    call.getOutputCollector().add(
            new Tuple(equiv.getId1(), equiv.getId2()));
  }
}
