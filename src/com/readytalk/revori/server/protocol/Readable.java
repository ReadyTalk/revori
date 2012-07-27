package com.readytalk.revori.server.protocol;

import java.io.IOException;

public interface Readable {
  public void readFrom(ReadContext context)
    throws IOException;
}
