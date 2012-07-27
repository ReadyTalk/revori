package com.readytalk.revori;

/**
 * These are the possible actions to take when an insert or update
 * introduces a row which conflicts with an existing row by matching
 * the same key of a primary key.
 */
public enum DuplicateKeyResolution {
  /**
   * Instructs the MyDBMS to silently skip the insert or update,
   * leaving the old row(s) intact.
   */
  Skip,
      
    /**
     * Instructs the MyDBMS to silently overwrite the old row with the
     * new one.
     */
    Overwrite,

    /**
     * Instructs the MyDBMS to throw a DuplicateKeyException when a
     * conflict is detected.
     */
    Throw;
}
