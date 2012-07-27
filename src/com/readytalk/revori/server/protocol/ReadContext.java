package com.readytalk.revori.server.protocol;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ReadContext {
  public final Map<Integer, Class<?>> classes = new HashMap<Integer, Class<?>>();
  public final Map<Integer, Object> objects = new HashMap<Integer, Object>();
  public final InputStream in;

  public ReadContext(InputStream in) {
    this.in = in;
  }
}