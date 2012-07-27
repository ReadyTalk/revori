package com.readytalk.revori.server;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;

public class StreamUtil {
  public static void writeString(OutputStream out, String s)
    throws IOException
  {
    if (s == null) {
      writeInt(out, 0);
    } else {
      byte[] bytes = s.getBytes("UTF-8");
      writeInt(out, bytes.length);
      out.write(bytes);
    }
  }

  public static String readString(InputStream in) throws IOException {
    int length = readInt(in);
    byte[] bytes = new byte[length];
    if (readFully(in, bytes, 0, length) != length) {
      throw new EOFException();
    }
    return new String(bytes, "UTF-8");
  }

  public static void writeInt(OutputStream out, int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v       ) & 0xFF);
  }

  public static int readInt(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();
    if (b4 == -1) throw new EOFException();
    return (int) ((b1 << 24) | (b2 << 16) | (b3 << 8) | (b4));
  }

  public static void writeLong(OutputStream out, long v) throws IOException {
    writeInt(out, (int) (v >>> 32) & 0xFFFFFFFF);
    writeInt(out, (int) (v       ) & 0xFFFFFFFF);
  }

  public static long readLong(InputStream in) throws IOException {
    long b1 = in.read();
    long b2 = in.read();
    long b3 = in.read();
    long b4 = in.read();
    long b5 = in.read();
    long b6 = in.read();
    long b7 = in.read();
    long b8 = in.read();
    if (b8 == -1) throw new EOFException();
    return (long) ((b1 << 56) | (b2 << 48) | (b3 << 40) | (b4 << 32) |
                   (b5 << 24) | (b6 << 16) | (b7 << 8) | (b8));
  }

  public static int readFully(InputStream in,
                              byte[] buffer,
                              int offset,
                              int length)
    throws IOException
  {
    int bytesRead = 0;
    while (bytesRead < length) {
      int n = in.read(buffer, bytesRead + offset, length - bytesRead);
      if (n == -1) break;
      bytesRead += n;
    }
    return bytesRead;
  }
}
