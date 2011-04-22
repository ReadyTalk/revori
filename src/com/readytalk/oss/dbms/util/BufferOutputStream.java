package com.readytalk.oss.dbms.util;

import java.io.ByteArrayOutputStream;

public class BufferOutputStream extends ByteArrayOutputStream {
  public byte[] getBuffer() {
    return buf;
  }
}
