package com.readytalk.oss.dbms;

public class ConflictResolvers {
  public static final ConflictResolver Restrict = new ConflictResolver() {
    public Object resolveConflict(Table table, Column column,
        Object[] primaryKeyValues, Object baseValue, Object leftValue,
        Object rightValue) {
      throw new RuntimeException("conflict occurred");
    }
  };
}
