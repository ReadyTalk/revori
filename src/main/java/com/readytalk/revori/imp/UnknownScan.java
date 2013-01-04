/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.List;

import com.google.common.collect.Lists;

class UnknownScan implements Scan {
  public static final UnknownScan Instance = new UnknownScan();

  public boolean isUseful() {
    return false;
  }

  public boolean isSpecific() {
    return false;
  }

  public boolean isUnknown() {
    return true;
  }

  public List<Interval> evaluate() {
    return Lists.newArrayList(Interval.Unbounded);
  }
}
