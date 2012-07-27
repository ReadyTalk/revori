package com.readytalk.revori.util;

import java.io.ByteArrayOutputStream;

public class BufferOutputStream extends ByteArrayOutputStream {
  public byte[] getBuffer() {
    return buf;
  }
}
