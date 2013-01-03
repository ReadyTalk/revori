package com.readytalk.revori.subscribe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.readytalk.revori.Revisions;
import com.readytalk.revori.server.RevisionServer;

/**
 * An instance of this class may be used to keep track of
 * "significant" revisions in the stream of revisions produced by a
 * RevisionServer.  Such revisions will be returned in sequence by
 * succesive calls to next(LinearRevision).  This is useful for
 * implementing streams where some revisions may have only partial
 * data, whereas others have complete data, and thus the latter are
 * sent preferentially to clients, with partial updates sent only when
 * after the latest complete update has been sent.
 */
public class DiffServer {
  private static final Object Head = new Object();

  private final RevisionServer server;
  private final Set<Runnable> listeners = new HashSet<Runnable>();
  private final Map<Object, LinearRevision> tags = new HashMap<Object, LinearRevision>();
  private final TreeMap<Long, LinearRevision> revisions = new TreeMap<Long, LinearRevision>();
  private long nextSequenceNumber = 0;
  private final LinearRevision tail = new LinearRevision
    (Revisions.Empty, nextSequenceNumber++);

  public DiffServer(final RevisionServer server) {
    this.server = server;

    server.registerListener(new Runnable() {
      public void run() {
        decrement(
          tags.put(
            Head,
            increment(
              new LinearRevision(
                server.head(),
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
  
  public void setTag(Object key) {
    decrement(tags.put(key, increment(tags.get(Head))));
  }

  public void removeTag(Object key) {
    decrement(tags.remove(key));
  }

  public Subscription register(final Runnable listener) {
    listeners.add(listener);

    return new Subscription() {
      public void cancel() {
        listeners.remove(listener);
      }
    };
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
