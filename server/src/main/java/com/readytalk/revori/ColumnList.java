/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import java.util.List;

import com.google.common.collect.ImmutableList;

public final class ColumnList {

  /**
   * The columns that this Row will contain values for.
   */
  public final List<Column<?>> columns;

  /**
   * This allows the DBMS implementation to attach some data
   * to allow it to insert rows more quickly.
   * With the current implementation, this will be a binary search
   * tree of columns.
   * Ick, I know...
   */
  public Object impData;

  public ColumnList(List<Column<?>> columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

  public ColumnList(Column<?>... columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

}