/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

public class Foldables {
  public static final Foldable<Integer> Count = new Foldable<Integer>() {
    public Integer add(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation + 1;
    }

    public Integer subtract(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation - 1;
    }
  };

  public static final Foldable<Integer> Sum = new Foldable<Integer>() {
    public Integer add(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation + (Integer) values[0];
    }

    public Integer subtract(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation - (Integer) values[0];
    }
  };
}
