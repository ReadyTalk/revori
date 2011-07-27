package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;

public interface Serializer<T> {
  public void writeTo(WriteContext context, T v) throws IOException;
}
