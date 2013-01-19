/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

interface SourceAdapter {
  public SourceIterator iterator(DefaultRevision base,
                                 NodeStack baseStack,
                                 DefaultRevision fork,
                                 NodeStack forkStack,
                                 ExpressionAdapter test,
                                 ExpressionContext expressionContext,
                                 boolean visitUnchanged);
  public void visit(SourceAdapterVisitor visitor);
  public void visit(ExpressionContext expressionContext,
                    ColumnReferenceAdapterVisitor visitor);
}
