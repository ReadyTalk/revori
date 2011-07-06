package com.readytalk.oss.dbms;

public enum Resolution {
  /**
   * Throw exception for existing keys
   */
  Insert,

  /**
   * Throw exception for non-existing keys
   */
  Update,

  /**
   * Insert or update where apropriate
   */
  Alter
}