package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;
import java.io.OutputStream;

import com.readytalk.oss.dbms.server.EpidemicServer;


public interface Writable {
  public void writeTo(EpidemicServer server, OutputStream out)
    throws IOException;
}
