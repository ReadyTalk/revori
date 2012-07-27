/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server.protocol;

import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.Map;

public class WriteContext {
  public final Map<Class<?>, Integer> classIDs = new IdentityHashMap<Class<?>, Integer>();
  public final Map<Object, Integer> objectIDs = new IdentityHashMap<Object, Integer>();
  public final OutputStream out;
  public int nextID;

  public WriteContext(OutputStream out) {
    this.out = out;
  }
}