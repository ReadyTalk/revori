package com.readytalk.revori;

public class ForeignKeyException extends RuntimeException {
  public ForeignKeyException() {
    super();
  }

  public ForeignKeyException(String message) {
    super(message);
  }
}
