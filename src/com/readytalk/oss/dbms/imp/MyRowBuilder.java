package com.readytalk.oss.dbms.imp;

class MyRowBuilder implements RowBuilder {

  private RevisionBuilder builder;
  private Object[] path;
  
  public RowBuilder update(Column key, Object value);

  public RowBuilder delete(Column key);
}