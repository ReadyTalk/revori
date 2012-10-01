/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

/**
 * These are the possible actions to take when an insert or update
 * introduces a row which conflicts with an existing row by matching
 * the same key of a primary key.
 */
public enum DuplicateKeyResolution {
  /**
   * Instructs the RevisionBuilder to silently skip the insert or
   * update, leaving the old row(s) intact.
   */
  Skip,
      
    /**
     * Instructs the RevisionBuilder to silently overwrite the old row
     * with the new one.
     */
    Overwrite,

    /**
     * Instructs the RevisionBuilder to throw a DuplicateKeyException
     * when a conflict is detected.
     */
    Throw;
}
