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
