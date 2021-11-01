package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import com.ycu.tang.msbplatform.batch.thrift.Data;
import com.ycu.tang.msbplatform.batch.thrift.PersonID;

public class MakeNormalizedPageview
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    PersonID newId = (PersonID) call.getArguments()
            .getObject(0);
    Data data = ((Data) call.getArguments().getObject(1))
            .deepCopy();
    if (newId != null) {
      data.getDataunit().getPage_view().setPerson(newId);
    }
    call.getOutputCollector().add(new Tuple(data));
  }
}
