package com.readytalk.oss.dbms;

public interface TableBuilder extends RevisionBuilder {
  
  /**
   * Prepares a RowBuilder to insert or update a row given by the specified 
   * primary key.
   *
   * If there is already row with a matching primary key in the table,
   * or there is no previous matching row, this method will act according
   * to the specified Resolution.
   */
  public RowBuilder row(Resolution resolution,
                           Object ... key);

  public void delete(Object ... key);
}