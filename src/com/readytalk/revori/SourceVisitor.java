package com.readytalk.revori;

/**
 * Implementations of this interface may be used to visit each node in
 * a source graph.
 */
public interface SourceVisitor {
  /**
   * Visit the specified source.
   */
  public void visit(Source s);
}
