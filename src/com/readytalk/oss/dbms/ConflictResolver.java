package com.readytalk.oss.dbms;

/**
 * An interface for resolving conflicts which accepts three versions
 * of a value -- the base version and two forks -- and returns a
 * value which resolves the conflict in an application-appropriate
 * way.
 */
public interface ConflictResolver {
  public Object resolveConflict(Table table,
                                Column column,
                                Object[] primaryKeyValues,
                                Object baseValue,
                                Object leftValue,
                                Object rightValue);
}
