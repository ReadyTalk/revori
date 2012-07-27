package com.readytalk.revori;

import com.readytalk.revori.imp.MyRevision;

/**
 * Uninstantiable utility class containing the empty database
 * revision.
 */
public class Revisions {
  private Revisions() { }

  /**
   * This is the empty database revision from which new revisions may
   * be derived via <code>Revision.builder()</code>.
   */
  public static final Revision Empty = MyRevision.Empty;
}
