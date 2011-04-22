package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.list;

import java.util.List;

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
    return list(Interval.Unbounded);
  }
}
