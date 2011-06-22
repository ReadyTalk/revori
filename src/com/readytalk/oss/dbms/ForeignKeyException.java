package com.readytalk.oss.dbms;

public class ForeignKeyException extends RuntimeException {
  public ForeignKeyException() {
    super();
  }

  public ForeignKeyException(String message) {
    super(message);
  }
}
