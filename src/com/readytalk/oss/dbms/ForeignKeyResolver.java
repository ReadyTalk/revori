package com.readytalk.oss.dbms;

public interface ForeignKeyResolver {
  public ForeignKey.Action handleBrokenReference
    (ForeignKey constraint,
     Object[] refererRowPrimaryKeyValues);
}
