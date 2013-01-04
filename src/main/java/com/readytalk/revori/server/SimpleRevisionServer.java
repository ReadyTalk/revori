/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Atomics;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.subscribe.Subscription;

public class SimpleRevisionServer implements RevisionServer {
  private final ConflictResolver conflictResolver;
  private final ForeignKeyResolver foreignKeyResolver;
  private final AtomicReference<Revision> head = Atomics.newReference(Revisions.Empty);
  public final AtomicReference<Set<Runnable>> listeners = Atomics.<Set<Runnable>>newReference(Sets.<Runnable>newHashSet());

  public SimpleRevisionServer(@Nullable ConflictResolver conflictResolver,
		  @Nullable ForeignKeyResolver foreignKeyResolver)
  {
    this.conflictResolver = conflictResolver;
    this.foreignKeyResolver = foreignKeyResolver;
  }

  public Revision head() {
    return head.get();
  }

  public void merge(Revision base, Revision fork) {
    if (base != fork || base != head.get()) {
      while (! head.compareAndSet(base, fork)) {
        Revision h = head.get();
        fork = base.merge
          (h, fork, conflictResolver, foreignKeyResolver);
        base = h;
      }

      for (Runnable listener: listeners.get()) {
        listener.run();
      }
    }
  }

  public Subscription registerListener(final Runnable listener) {
    while (true) {
      Set<Runnable> oldListeners = listeners.get();
      Set<Runnable> newListeners = Sets.newHashSet(oldListeners); 
      newListeners.add(listener);
      if (listeners.compareAndSet(oldListeners, newListeners)) {
        break;
      }
    }
    listener.run();

    return new Subscription() {
      public void cancel() {
        while (true) {
          Set<Runnable> oldListeners = listeners.get();
          Set<Runnable> newListeners = Sets.newHashSet(oldListeners); 
          newListeners.remove(listener);
          if (listeners.compareAndSet(oldListeners, newListeners)) {
            break;
          }
        }
      }
    };
  }

}
