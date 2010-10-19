package com.readytalk.oss.dbms.imp;

import java.io.ByteArrayOutputStream;

public class BufferOutputStream extends ByteArrayOutputStream {
  public byte[] getBuffer() {
    return buf;
  }
}
