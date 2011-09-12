package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.cols;

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

  public static final Column<Table> TableColumn
    = new Column<Table>(Table.class,
                 "TableColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column<Index> IndexColumn
    = new Column<Index>(Index.class,
                 "IndexColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table IndexTable
    = new Table(cols(TableColumn, IndexColumn),
                "IndexTable.Constants.imp.dbms.oss.readytalk.com", Short.MIN_VALUE);

  public static final Column<View> ViewColumn
    = new Column<View>(View.class,
                 "ViewColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table ViewTable
    = new Table(cols(TableColumn, ViewColumn),
                "ViewTable.Constants.imp.dbms.oss.readytalk.com");

  public static final Column<Table> ForeignKeyRefererColumn
    = new Column<Table>(Table.class,
     "ForeignKeyRefererColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column<Table> ForeignKeyReferentColumn
    = new Column<Table>(Table.class,
     "ForeignKeyReferentColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Column<ForeignKey> ForeignKeyColumn
    = new Column<ForeignKey>(ForeignKey.class,
                 "ForeignKeyColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table ForeignKeyTable
    = new Table
    (cols(ForeignKeyColumn),
     "ForeignKeyTable.Constants.imp.dbms.oss.readytalk.com");

  public static final Index ForeignKeyRefererIndex
    = new Index(ForeignKeyTable, cols(ForeignKeyRefererColumn));

  public static final Index ForeignKeyReferentIndex
    = new Index(ForeignKeyTable, cols(ForeignKeyReferentColumn));
}
