package com.ycu.tang.msbplatform.batch.function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import com.ycu.tang.msbplatform.batch.thrift.Data;
import com.ycu.tang.msbplatform.batch.thrift.PageID;
import com.ycu.tang.msbplatform.batch.thrift.PageViewEdge;

public class ExtractPageViewFields
        extends CascalogFunction {
  public void operate(FlowProcess process, FunctionCall call) {
    Data data = (Data) call.getArguments().getObject(0);
    PageViewEdge pageview = data.getDataunit()
            .getPage_view();
    if (pageview.getPage().getSetField() ==
            PageID._Fields.URL) {
      call.getOutputCollector().add(new Tuple(
              pageview.getPage().getUrl(),
              pageview.getPerson(),
              data.getPedigree().getTrue_as_of_secs()
      ));
    }
  }
}
