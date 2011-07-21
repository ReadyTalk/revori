package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;

public interface Serializer {
  public void writeTo(WriteContext context, Object v) throws IOException;
}
