namespace java com.ycu.tang.msbplatform.batch.thrift

union PersonID {
  1: string cookie;
  2: i64 user_id;
}

union PageID {
  1: string url;
}