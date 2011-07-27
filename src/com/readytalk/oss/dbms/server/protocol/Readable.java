package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;
import java.io.InputStream;

public interface Readable {
  public void readFrom(InputStream in)
    throws IOException;
}
