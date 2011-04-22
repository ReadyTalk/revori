package com.readytalk.oss.dbms.imp;

import java.util.List;

interface Scan {
  public boolean isUseful();
  public boolean isSpecific();
  public boolean isUnknown();
  public List<Interval> evaluate();
}
