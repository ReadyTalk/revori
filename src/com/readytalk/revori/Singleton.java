/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

public final class Singleton implements Comparable<Singleton> {
  public static Singleton Instance = new Singleton();

  private Singleton() { }

  public int compareTo(Singleton o) {
    return 0;
  }
}
