/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

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
