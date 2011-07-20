package com.readytalk.oss.dbms;

public interface TableBuilder {
  
  /**
   * Prepares a RowBuilder to insert or update a row given by the specified 
   * primary key.
   * @return said row builder
   */
  public RowBuilder row(Comparable ... key);

  /**
   * Deletes the row with the specified primary key.
   * @return self
   */
  public TableBuilder delete(Comparable ... key);

  /**
   * Indicate that no further updates will
   * be performed on this TableBuilder.
   * @return the parent RevisionBuilder
   */
  public RevisionBuilder up();
}
