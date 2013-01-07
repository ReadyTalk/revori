/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import javax.annotation.Nullable;

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
                                @Nullable Object baseValue,
                                Object leftValue,
                                Object rightValue);
}
