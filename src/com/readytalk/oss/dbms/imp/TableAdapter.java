package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.ColumnReference;

class TableAdapter implements SourceAdapter {
  public final TableReference tableReference;

  public TableAdapter(TableReference tableReference) {
    this.tableReference = tableReference;
  }

  public TableIterator iterator(MyRevision base,
                                NodeStack baseStack,
                                MyRevision fork,
                                NodeStack forkStack,
                                ExpressionAdapter test,
                                ExpressionContext expressionContext,
                                boolean visitUnchanged)
  {
    return new TableIterator
      (tableReference, base, baseStack, fork, forkStack, test,
       expressionContext, visitUnchanged);
  }

  public void visit(SourceAdapterVisitor visitor) {
    visitor.visit(this);
  }

  public void visit(ExpressionContext expressionContext,
                    ColumnReferenceAdapterVisitor visitor)
  {
    for (ColumnReferenceAdapter r: expressionContext.columnReferences) {
      if (r.tableReference == tableReference) {
        visitor.visit(r);
      }
    }
  }
}
