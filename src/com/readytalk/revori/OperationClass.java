/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

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
