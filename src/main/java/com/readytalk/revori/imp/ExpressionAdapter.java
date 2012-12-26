/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

interface ExpressionAdapter {
  public void visit(ExpressionAdapterVisitor visitor);
  public Object evaluate(boolean convertDummyToNull);
  public Scan makeScan(ColumnReferenceAdapter reference);
  public Class type();
}
