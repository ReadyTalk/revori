package com.readytalk.oss.dbms;

public interface TableBuilder extends RevisionBuilder {
  
  /**
   * Prepares a RowBuilder to insert or update a row given by the specified 
   * primary key.  This is equivalent to modify(duplicateKeyResolution,
   * key, 0, key.length).
   *
   * If there is already row with a matching primary key in the table,
   * this method will act according to the specified
   * DuplicateKeyResolution.
   */
  public RowBuilder alter(Resolution resolution,
                           Object ... key);

  public void delete(Object ... key);
}