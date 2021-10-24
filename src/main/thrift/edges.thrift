namespace java com.ycu.tang.msbplatform.batch.thrift

include "nodes.thrift"

struct EquivEdge {
  1: required nodes.PersonID id1;
  2: required nodes.PersonID id2;
}

struct PageViewEdge {
  1: required nodes.PersonID person;
  2: required nodes.PageID page;
  3: required i64 nonce;
}