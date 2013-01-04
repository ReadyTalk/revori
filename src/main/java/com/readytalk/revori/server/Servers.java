/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.server.protocol.Readable;
import com.readytalk.revori.subscribe.Subscription;

public class Servers {
  public static RevisionServer asynchronousRevisionServer
    (final RevisionServer server,
     ConflictResolver conflictResolver,
     ForeignKeyResolver foreignKeyResolver,
     final TaskHandler handler)
  {
    return new AsynchronousRevisionServer
      (server, conflictResolver, foreignKeyResolver, handler);
  }

  public static NetworkServer asynchronousNetworkServer
    (final NetworkServer server,
     ConflictResolver conflictResolver,
     ForeignKeyResolver foreignKeyResolver,
     final TaskHandler handler)
  {
    return new AsynchronousNetworkServer
      (server, conflictResolver, foreignKeyResolver, handler);
  }

  public static Subscription bridge(final RevisionServer a,
                                    final RevisionServer b)
  {
    final Subscription sa = a.registerListener(new Runnable() {
        Revision base = Revisions.Empty;

        public void run() {
          b.merge(base, a.head());
          base = b.head();
        }
      });

    final Subscription sb = b.registerListener(new Runnable() {
        Revision base = Revisions.Empty;

        public void run() {
          a.merge(base, b.head());
          base = a.head();
        }
      });

    return new Subscription() {
      public void cancel() {
        sa.cancel();
        sb.cancel();
      }
    };
  }

  private static class AsynchronousRevisionServer implements RevisionServer {
    protected final RevisionServer server;
    private final ConflictResolver conflictResolver;
    private final ForeignKeyResolver foreignKeyResolver;
    private final List<Runnable> listeners = Lists.newArrayList();
    private Revision head;
    private Revision base;

    public AsynchronousRevisionServer
      (final RevisionServer server,
       final ConflictResolver conflictResolver,
       final ForeignKeyResolver foreignKeyResolver,
       final TaskHandler handler)
    {
      this.server = server;
      this.conflictResolver = conflictResolver;
      this.foreignKeyResolver = foreignKeyResolver;

      head = base = server.head();

      server.registerListener(new Runnable() {
          public void run() {
            //            new Exception().printStackTrace();
            handler.handleTask(new Runnable() {
                public void run() {
                  pull();
                }
              });
          }
        });
    }

    public Revision head() {
      return head;
    }

    public void merge(Revision base, Revision fork) {
      if (base != fork || base != head) {
        head = base.merge(head, fork, conflictResolver, foreignKeyResolver);

        for (Runnable listener: listeners) {
          listener.run();
        }

        server.merge(this.base, head);

        pull();
      }
    }

    private void pull() {
      Revision fork = server.head();
      if (base != fork || base != head) {
        head = base = base.merge
          (head, fork, conflictResolver, foreignKeyResolver);

        for (Runnable listener: listeners) {
          listener.run();
        }
      }
    }

    public Subscription registerListener(final Runnable listener) {
      listeners.add(listener);

      return new Subscription() {
        public void cancel() {
          listeners.remove(listener);
        }
      };
    }
  }

  private static class AsynchronousNetworkServer
    extends AsynchronousRevisionServer
    implements NetworkServer
  {
    private final NetworkServer networkServer;

    public AsynchronousNetworkServer(final NetworkServer server,
                                     ConflictResolver conflictResolver,
                                     ForeignKeyResolver foreignKeyResolver,
                                     final TaskHandler handler)
    {
      super(server, conflictResolver, foreignKeyResolver, handler);

      this.networkServer = server;
    }

    public void accept(NodeID source, Readable message) {
      networkServer.accept(source, message);
    }

    public void updateView(Set<NodeID> directlyConnectedNodes) {
      networkServer.updateView(directlyConnectedNodes);
    }
  }

  public interface TaskHandler {
    public void handleTask(Runnable task);
  }
}
