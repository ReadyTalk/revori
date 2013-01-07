/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import javax.annotation.Nullable;

public interface Foldable<T> {
  public T base();

  public T add(@Nullable T accumulation, Object ... values);
  
  public T subtract(@Nullable T accumulation, Object ... values);
}
