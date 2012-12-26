/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

/**
 * Type representing a query source (e.g. table reference or join)
 * from which to derive a query result.
 */
public interface Source extends Comparable<Source> {
  /**
   * Visit this source and any subsources by calling
   * SourceVisitor.visit(Source) with each.
   */
  public void visit(SourceVisitor visitor);
}
