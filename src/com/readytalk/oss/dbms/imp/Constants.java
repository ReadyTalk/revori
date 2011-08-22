package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.list;

import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.View;
import com.readytalk.oss.dbms.ForeignKey;

class Constants {
  public static final int TableDataDepth = 0;
  public static final int IndexDataDepth = 1;
  public static final int IndexDataBodyDepth = 2;
  public static final int MaxIndexDataBodyDepth = 8;
  public static final int MaxDepth
    = IndexDataBodyDepth + MaxIndexDataBodyDepth;

  public static final Column TableColumn
    = new Column(Table.class,
                 "TableColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column IndexColumn
    = new Column(Index.class,
                 "IndexColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table IndexTable
    = new Table(list(TableColumn, IndexColumn),
                "IndexTable.Constants.imp.dbms.oss.readytalk.com");

  public static final Column ViewColumn
    = new Column(View.class,
                 "ViewColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table ViewTable
    = new Table(list(TableColumn, ViewColumn),
                "ViewTable.Constants.imp.dbms.oss.readytalk.com");

  public static final Column ForeignKeyRefererColumn
    = new Column
    (Table.class,
     "ForeignKeyRefererColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column ForeignKeyReferentColumn
    = new Column
    (Table.class,
     "ForeignKeyReferentColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column ForeignKeyColumn
    = new Column(ForeignKey.class,
                 "ForeignKeyColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table ForeignKeyTable
    = new Table
    (list(ForeignKeyColumn),
     "ForeignKeyTable.Constants.imp.dbms.oss.readytalk.com");

  public static final Index ForeignKeyRefererIndex
    = new Index(ForeignKeyTable, list(ForeignKeyRefererColumn));

  public static final Index ForeignKeyReferentIndex
    = new Index(ForeignKeyTable, list(ForeignKeyReferentColumn));
}
