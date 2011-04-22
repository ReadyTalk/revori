package com.readytalk.oss.dbms;

/**
 * This is the abstract superclass of classes representing updates to
 * a database revision.
 */
public interface PatchTemplate {
  /**
   * Returns the number of parameters which must be resolved when this
   * template is applied to a revision.
   */
  public int parameterCount();
}
