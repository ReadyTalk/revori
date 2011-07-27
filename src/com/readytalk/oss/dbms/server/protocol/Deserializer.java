package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;

public interface Deserializer<T> {
  public T readFrom(ReadContext context, Class<? extends T> c) throws IOException;

}
