package com.readytalk.oss.dbms.server.protocol;

import java.io.IOException;

public interface Deserializer {
  public Object readFrom(ReadContext context, Class c) throws IOException;

}
