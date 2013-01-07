/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class Util {
  
  public static void massInsert(RevisionBuilder builder, InsertTemplate insert, QueryResult result) {
    int count = insert.parameterCount();
    Object[] params = new Object[count];
    QueryResult.Type t;
    while((t = result.nextRow()) != QueryResult.Type.End) {
      if(t != QueryResult.Type.Deleted) {
        for(int i = 0; i < count; i++) {
          params[i] = result.nextItem();  
        }
        builder.apply(insert, params);
      }
    }
  }

}
