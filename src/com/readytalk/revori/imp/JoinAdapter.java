package com.readytalk.revori.imp;

import com.readytalk.revori.Join;

class JoinAdapter implements SourceAdapter {
  public final Join.Type type;
  public final SourceAdapter left;
  public final SourceAdapter right;

  public JoinAdapter(Join.Type type,
                     SourceAdapter left,
                     SourceAdapter right)
  {
    this.type = type;
    this.left = left;
    this.right = right;   
  }

  public SourceIterator iterator(MyRevision base,
                                 NodeStack baseStack,
                                 MyRevision fork,
                                 NodeStack forkStack,
                                 ExpressionAdapter test,
                                 ExpressionContext expressionContext,
                                 boolean visitUnchanged)
  {
    return new JoinIterator
      (this, base, baseStack, fork, forkStack, test, expressionContext,
       visitUnchanged);
  }

  public void visit(SourceAdapterVisitor visitor) {
    left.visit(visitor);
    right.visit(visitor);
  }

  public void visit(ExpressionContext expressionContext,
                    ColumnReferenceAdapterVisitor visitor)
  {
    left.visit(expressionContext, visitor);
    right.visit(expressionContext, visitor);
  }
}
