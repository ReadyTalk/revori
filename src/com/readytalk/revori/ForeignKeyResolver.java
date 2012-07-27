package com.readytalk.revori;

public interface ForeignKeyResolver {
  public enum Action {
    Restrict, Delete;
  }

  public Action handleBrokenReference(ForeignKey constraint,
                                      Object[] refererRowPrimaryKeyValues);
}
