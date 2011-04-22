package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.ColumnReference;

interface ColumnReferenceAdapterVisitor {
  public void visit(ColumnReferenceAdapter r);
}
