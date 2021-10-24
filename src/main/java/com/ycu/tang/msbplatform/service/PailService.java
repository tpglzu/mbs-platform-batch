package com.ycu.tang.msbplatform.service;

import backtype.hadoop.pail.Pail;
import com.ycu.tang.msbplatform.batch.Properties;
import com.ycu.tang.msbplatform.service.pailstructure.SplitDataPailStructure;
import com.ycu.tang.msbplatform.batch.thrift.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class PailService {

  @Autowired
  protected Properties properties;

  private Configuration hadoopConf;

  public void writeData(String pailName, Data data) throws IOException {
    Pail pail = null;
    if(!isPailExists(pailName)){
      pail = createPail(pailName);
    }else{
      pail = new Pail(getFs(), pailName);
    }
    Pail.TypedRecordOutputStream out = pail.openWrite();
    out.writeObject(data);
    out.close();
  }

  public final void writeData(String pailName, List<Data> data) throws IOException {
    Pail pail = null;
    if(!isPailExists(pailName)){
      pail = createPail(pailName);
    }else{
      pail = new Pail(getFs(), pailName);
    }
    Pail.TypedRecordOutputStream out = pail.openWrite();
    out.writeObjects(data.toArray());
    out.close();
  }

  public Pail createPail(String pailName) throws IOException {
    FileSystem fs = new Path(pailName).getFileSystem(getHadoopConf());
    return Pail.create(getFs(), pailName, new SplitDataPailStructure());
  }

  public Pail getPail(String pailName) throws IOException {
    Pail pail = null;
    if(!isPailExists(pailName)){
      pail = createPail(pailName);
    }else{
      pail = new Pail(getFs(), pailName);
    }
    return pail;
  }

  public boolean isPailExists(String pailName) throws IOException {
    return getFs().exists(new Path(pailName));
  }

  public Configuration getHadoopConf(){
    if(hadoopConf == null){
      hadoopConf = new Configuration();
      // TODO local
//      hadoopConf.set("fs.default.name", properties.getNamenodeUrl());
    }
    return hadoopConf;
  }

  public FileSystem getFs() throws IOException {
    return FileSystem.get(getHadoopConf());
  }
}
