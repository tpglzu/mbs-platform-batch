namespace java com.ycu.tang.msbplatform.batch.thrift

include "nodes.thrift"

union PagePropertyValue {
  1: i32 page_view;
}

struct PageProperty {
  1: required nodes.PageID id;
  2: required PagePropertyValue property;
}

struct Location {
  1: optional string city;
  2: optional string state;
  3: optional string country;
}

enum GenderType {
  MALE = 1,
  FEMALE = 2
}

union PersonPropertyValue {
  1: string full_name;
  2: GenderType gender;
  3: Location location;
}

struct PersonProperty {
  1: required nodes.PersonID id;
  2: required PersonPropertyValue property;
}