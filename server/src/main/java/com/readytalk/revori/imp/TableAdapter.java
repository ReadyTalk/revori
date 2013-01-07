/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.TableReference;

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
