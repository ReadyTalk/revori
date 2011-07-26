package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;
import java.io.InputStream;

import com.readytalk.oss.dbms.server.EpidemicServer;

public interface Readable {
  public void readFrom(InputStream in)
    throws IOException;
}
