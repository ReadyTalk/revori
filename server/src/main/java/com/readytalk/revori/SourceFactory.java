/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

public class SourceFactory {
  public static Join leftJoin(Source left, Source right) {
    return new Join(Join.Type.LeftOuter, left, right);
  }

  public static Join innerJoin(Source left, Source right) {
    return new Join(Join.Type.Inner, left, right);
  }

  public static TableReference reference(Table table) {
    return new TableReference(table);
  }
}
