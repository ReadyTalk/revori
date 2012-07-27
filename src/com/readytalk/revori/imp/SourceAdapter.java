package com.readytalk.revori.imp;

interface SourceAdapter {
  public SourceIterator iterator(MyRevision base,
                                 NodeStack baseStack,
                                 MyRevision fork,
                                 NodeStack forkStack,
                                 ExpressionAdapter test,
                                 ExpressionContext expressionContext,
                                 boolean visitUnchanged);
  public void visit(SourceAdapterVisitor visitor);
  public void visit(ExpressionContext expressionContext,
                    ColumnReferenceAdapterVisitor visitor);
}
