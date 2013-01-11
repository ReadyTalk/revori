/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

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