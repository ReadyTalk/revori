package com.readytalk.revori.server;

import java.util.Set;

import com.readytalk.revori.Revision;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.subscribe.Subscription;

public class BufferServer implements RevisionServer {
  private final Set<BufferServer> dirtySet;
  private final RevisionServer server;
  private boolean dirty;
  private Revision base;
  private Revision head;
  private Revision updateBase;
  ConflictResolver conflictResolver;
  ForeignKeyResolver foreignKeyResolver;

  public BufferServer(Set<BufferServer> dirtySet,
                      RevisionServer server,
                      ConflictResolver conflictResolver,
                      ForeignKeyResolver foreignKeyResolver)
  {
    this.dirtySet = dirtySet;
    this.server = server;
  }

  public RevisionServer server() {
    return server;
  }

  public Revision latestHead() {
    if (base == null) {
      base = head = server.head();
    } else {
      Revision serverHead = server.head();
      merge(base, serverHead);
      base = serverHead;
    }
    return head;
  }

  public Revision head() {
    if (base == null) {
      base = head = server.head();
    }
    return head;
  }

  public void merge(Revision base, Revision fork) {
    head = base.merge
      (head, fork, conflictResolver, foreignKeyResolver);

    if (! dirty) {
      dirty = true;
      dirtySet.add(this);
    }
  }

  public void flush(boolean remove) {
    server.merge(base, head);
    base = head = null;
    dirty = false;
    if (remove) {
      dirtySet.remove(this);
    }
  }

  public void flush() {
    flush(true);
  }

  public Subscription registerListener(Runnable listener) {
    throw new UnsupportedOperationException();
  }

}
