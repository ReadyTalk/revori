/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import javax.annotation.concurrent.Immutable;

/**
 * Exception thrown when an insert or update introduces a row which
 * conflicts with an existing row by matching the same key of a
 * primary key.
 */
@Immutable
public class DuplicateKeyException extends RuntimeException {
	private static final long serialVersionUID = 1L;
}
