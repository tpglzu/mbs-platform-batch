package com.ycu.tang.msbplatform.service.pailstructure;

import backtype.hadoop.pail.PailStructure;
import org.apache.thrift.*;
import org.apache.thrift.transport.TTransportException;

public abstract class ThriftPailStructure<T extends Comparable> implements PailStructure<T> {
  private transient TSerializer ser;
  private transient TDeserializer des;

  private TSerializer getSerializer() throws TTransportException {
    if (ser == null) ser = new TSerializer();
    return ser;
  }

  private TDeserializer getDeserializer() throws TTransportException {
    if (des == null) des = new TDeserializer();
    return des;
  }

  @Override
  public byte[] serialize(T obj) {
    try {
      return getSerializer().serialize((TBase) obj);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T deserialize(byte[] record) {
    T ret = createThriftObject();
    try {
      getDeserializer().deserialize((TBase) ret, record);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }

  protected abstract T createThriftObject();
}
