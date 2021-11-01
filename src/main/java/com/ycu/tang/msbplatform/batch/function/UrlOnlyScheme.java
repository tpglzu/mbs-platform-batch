package com.ycu.tang.msbplatform.batch.function;

import com.ycu.tang.msbplatform.batch.BatchWorkflow;
import elephantdb.partition.ShardingScheme;

import java.io.UnsupportedEncodingException;

public class UrlOnlyScheme implements ShardingScheme {

  public int shardIndex(byte[] shardKey, int shardCount) {
    String url = getUrlFromSerializedKey(shardKey);
    return url.hashCode() % shardCount;
  }

    private String getUrlFromSerializedKey(byte[] ser) {
        try {
            String key = new String(ser, "UTF-8");
            return key.substring(0, key.lastIndexOf("/"));
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
