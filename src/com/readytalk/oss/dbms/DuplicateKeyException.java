package com.readytalk.oss.dbms;

/**
 * Exception thrown when an insert or update introduces a row which
 * conflicts with an existing row by matching the same key of a
 * primary key.
 */
public class DuplicateKeyException extends RuntimeException { }
