package com.readytalk.oss.dbms.server.protocol;

import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.Map;

import com.readytalk.oss.dbms.server.EpidemicServer;


public class WriteContext {
  public final Map<Class, Integer> classIDs = new IdentityHashMap();
  public final Map<Object, Integer> objectIDs = new IdentityHashMap();
  public final OutputStream out;
  public int nextID;

  public WriteContext(OutputStream out) {
    this.out = out;
  }
}