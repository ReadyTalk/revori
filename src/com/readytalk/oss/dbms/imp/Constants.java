package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.list;

import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Index;

class Constants {
  public static final int TableDataDepth = 0;
  public static final int IndexDataDepth = 1;
  public static final int IndexDataBodyDepth = 2;
  public static final int MaxIndexDataBodyDepth = 8;
  public static final int MaxDepth
    = IndexDataBodyDepth + MaxIndexDataBodyDepth;

  public static final Column TableColumn
    = new Column(Table.class,
                 "com.readytalk.oss.dbms.imp.Constants.TableColumn");

  public static final Column IndexColumn
    = new Column(Index.class,
                 "com.readytalk.oss.dbms.imp.Constants.IndexColumn");

  public static final Table IndexTable
    = new Table(list(TableColumn, IndexColumn),
                "com.readytalk.oss.dbms.imp.Constants.IndexTable");
}
