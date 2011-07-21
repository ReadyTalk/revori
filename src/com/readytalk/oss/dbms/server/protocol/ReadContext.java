package com.readytalk.oss.dbms.server.protocol;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.readytalk.oss.dbms.server.EpidemicServer;


public class ReadContext {
  public final Map<Integer, Class> classes = new HashMap();
  public final Map<Integer, Object> objects = new HashMap();
  public final InputStream in;
  public final EpidemicServer server;

  public ReadContext(InputStream in, EpidemicServer server) {
    this.in = in;
    this.server = server;
  }
}