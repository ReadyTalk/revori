package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.ExpressionVisitor;

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

  public Class type() {
    return column.type;
  }
}
