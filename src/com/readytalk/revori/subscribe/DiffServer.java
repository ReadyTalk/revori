package com.readytalk.revori.subscribe;

import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.Set;
import java.util.HashSet;

import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.server.BufferServer;

public class DiffServer {
  private static final Object Head = new Object();

  private final BufferServer server;
  private final Set<Runnable> listeners = new HashSet<Runnable>();
  private final Map<Object, LinearRevision> tags = new HashMap<Object, LinearRevision>();
  private final TreeMap<Long, LinearRevision> revisions = new TreeMap<Long, LinearRevision>();
  private long nextSequenceNumber = 0;
  private final LinearRevision tail = new LinearRevision
    (Revisions.Empty, nextSequenceNumber++);

  public DiffServer(final BufferServer server) {
    this.server = server;

    server.server().registerListener(new Runnable() {
      public void run() {
        decrement(
          tags.put(
            Head,
            increment(
              new LinearRevision(
                server.server().head(),
                nextSequenceNumber++))));
        
        for (Runnable listener: listeners) {
          listener.run();
        }
      }
    });
  }

  private LinearRevision increment(LinearRevision r) {
    if (r.referenceCount++ == 0) {
      revisions.put(r.sequenceNumber, r);
    }
    return r;
  }

  private LinearRevision decrement(LinearRevision r) {
    if (r != null) {
      if (r.referenceCount <= 0) throw new RuntimeException();

      if (--r.referenceCount == 0) {
        revisions.remove(r.sequenceNumber);
      }
    }
    return r;
  }
  
  public BufferServer server() {
    return server;
  }

  public void setTag(Object key) {
    decrement(tags.put(key, increment(tags.get(Head))));
  }

  public void removeTag(Object key) {
    decrement(tags.remove(key));
  }

  public void register(Runnable listener) {
    listeners.add(listener);
  }

  public void unregister(Runnable listener) {
    listeners.remove(listener);
  }

  public LinearRevision next(LinearRevision base) {
    SortedMap<Long, LinearRevision> tail = revisions.tailMap
      (base.sequenceNumber + 1);

    return tail.isEmpty() ? null : tail.values().iterator().next();
  }

  public LinearRevision tail() {
    return tail;
  }
}