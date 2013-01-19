/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

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

  public SourceIterator iterator(DefaultRevision base,
                                 NodeStack baseStack,
                                 DefaultRevision fork,
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
