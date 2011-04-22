package com.readytalk.oss.dbms;

/**
 * These are the possible classes of UnaryOperationTypes and
 * BinaryOperationTypes.
 */
public enum OperationClass {
  /**
   * Indicates a comparison (e.g. equal, less than, etc.) operation.
   */
  Comparison,

    /**
     * Indicates a boolean (e.g. and, or, etc.) operation.
     */
    Boolean;
}
