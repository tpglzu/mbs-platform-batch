namespace java com.ycu.tang.msbplatform.batch.thrift

include "properties.thrift"
include "edges.thrift"

union DataUnit {
  1: properties.PersonProperty person_property;
  2: properties.PageProperty page_property;
  3: edges.EquivEdge equiv;
  4: edges.PageViewEdge page_view;
}

struct Pedigree {
  1: required i32 true_as_of_secs;
}

struct Data {
  1: required Pedigree pedigree;
  2: required DataUnit dataunit;
}

enum Source {
  SELF = 1,
  BACKTYPE = 2
}

struct ExternalDataSystem {
}

struct PageViewSystem {
}

union OrigSystem {
  1: PageViewSystem page_view;
  2: ExternalDataSystem external_data;
}