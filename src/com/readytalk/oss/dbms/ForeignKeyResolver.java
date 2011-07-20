package com.readytalk.oss.dbms;

public interface ForeignKeyResolver {
  public enum Action {
    Restrict, Delete;
  }

  public Action handleBrokenReference(ForeignKey constraint,
                                      Object[] refererRowPrimaryKeyValues);
}
