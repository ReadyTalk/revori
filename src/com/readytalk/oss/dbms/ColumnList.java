package com.readytalk.oss.dbms;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;

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
    this.columns = Collections.unmodifiableList(columns);
  }

  public ColumnList(Column<?>... columns) {
    this.columns = Collections.unmodifiableList(new ArrayList<Column<?>>(Arrays.asList(columns)));
  }

}