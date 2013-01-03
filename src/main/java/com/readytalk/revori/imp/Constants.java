/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.cols;

import javax.annotation.concurrent.Immutable;

import com.readytalk.revori.Column;
import com.readytalk.revori.ForeignKey;
import com.readytalk.revori.Index;
import com.readytalk.revori.Table;
import com.readytalk.revori.View;

@Immutable
public class Constants {
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
                "IndexTable.Constants.imp.dbms.oss.readytalk.com", 0, false);

  public static final Column<View> ViewColumn
    = new Column<View>(View.class,
                 "ViewColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Table ViewTable
    = new Table(cols(TableColumn, ViewColumn),
                "ViewTable.Constants.imp.dbms.oss.readytalk.com", 0, false);

  public static final Column<Table> ViewTableColumn = new Column<Table>
    (Table.class, "ViewTableColumn.Constants.imp.dbms.oss.readytalk.com");

  public static final Index ViewTableIndex = new Index
    (ViewTable, cols(ViewTableColumn));

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

  public static boolean serializable(Table table, Object key, int depth) {
    if (depth == 0) {
      return table.serializable;
    } else {
      return depth < table.primaryKey.columns.size() + IndexDataBodyDepth
        || ((Column) key).serializable;
    }
  }
}
