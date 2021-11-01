package com.ycu.tang.msbplatform;

import backtype.hadoop.pail.Pail;
import com.ycu.tang.msbplatform.batch.thrift.Data;

import java.io.IOException;

public class Tester {
  public static void main(String[] args) throws IOException {
    Pail<Data> pail = new Pail<>("/tmp/swa/equivs1");
    for(Data l : pail) {
      System.out.println(l);
    }
  }
}
