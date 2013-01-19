/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.Column;
import com.readytalk.revori.TableReference;

class ColumnReferenceAdapter implements ExpressionAdapter {
  public final TableReference tableReference;
  public final Column<?> column;
  public Object value = Compare.Undefined;

  public ColumnReferenceAdapter(TableReference tableReference,
                                Column<?> column)
  {
    this.tableReference = tableReference;
    this.column = column;
  }

  public void visit(ExpressionAdapterVisitor visitor) {
    visitor.visit(this);
  }

  public Object evaluate(boolean convertDummyToNull) {
    return convertDummyToNull && value == Compare.Dummy ? null : value;
  }

  public Scan makeScan(ColumnReferenceAdapter reference) {
    throw new UnsupportedOperationException();
  }

  public Class<?> type() {
    return column.type;
  }
}
