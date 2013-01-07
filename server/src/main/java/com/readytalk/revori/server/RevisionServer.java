/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import com.readytalk.revori.Revision;
import com.readytalk.revori.subscribe.Subscription;

public interface RevisionServer {
  public Revision head();

  public void merge(Revision base, Revision fork);

  public Subscription registerListener(Runnable listener);
}
