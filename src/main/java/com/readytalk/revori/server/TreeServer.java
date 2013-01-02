/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import static com.readytalk.revori.util.Util.set;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.readytalk.revori.Column;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Table;
import com.readytalk.revori.server.protocol.Readable;
import com.readytalk.revori.subscribe.Subscription;

public class TreeServer implements NetworkServer {
  private final RevisionServer localServer;
  private final Map<NodeID,ServerState> serverStates = new HashMap();
  private final NodeConflictResolver conflictResolver;
  private final ForeignKeyResolver foreignKeyResolver;
  private final Network network;
  private final NodeID self;
  private final UUID instance;

  public TreeServer(final NodeConflictResolver conflictResolver,
                    ForeignKeyResolver foreignKeyResolver,
                    Network network,
                    final NodeID self)
  {
    this(conflictResolver, foreignKeyResolver, network, self,
         UUID.randomUUID());
  }

  public TreeServer(final NodeConflictResolver conflictResolver,
                    ForeignKeyResolver foreignKeyResolver,
                    Network network,
                    final NodeID self,
                    UUID instance)
  {
    localServer = new SimpleRevisionServer(new ConflictResolver() {
        public Object resolveConflict(Table table,
                                      Column column,
                                      Object[] primaryKeyValues,
                                      Object baseValue,
                                      Object leftValue,
                                      Object rightValue)
        {
          return conflictResolver.resolveConflict
            (self,
             self,
             table,
             column,
             primaryKeyValues,
             baseValue,
             leftValue,
             rightValue);
        }
      }, foreignKeyResolver);

    this.conflictResolver = conflictResolver;
    this.foreignKeyResolver = foreignKeyResolver;
    this.network = network;
    this.self = self;
    this.instance = instance;
  }

  public Revision head() {
    return localServer.head();
  }

  public void merge(Revision base, Revision fork) {
    localServer.merge(base, fork);
  }

  public Subscription registerListener(final Runnable listener) {
    return localServer.registerListener(listener);
  }

  public void accept(NodeID source, Readable message) {
    serverStates.get(source).server.accept(source, message);
  }

  public void updateView(Set<NodeID> directlyConnectedNodes) {
    for (Iterator<Map.Entry<NodeID,ServerState>> it
           = serverStates.entrySet().iterator();
         it.hasNext();)
    {
      Map.Entry<NodeID,ServerState> e = it.next();
      if (! directlyConnectedNodes.contains(e.getKey())) {
        e.getValue().subscription.cancel();
        it.remove();
      }
    }

    for (NodeID node: directlyConnectedNodes) {
      ServerState s = serverStates.get(node);
      if (s == null) {
        NetworkServer server = new EpidemicServer
          (conflictResolver, foreignKeyResolver, network, self, instance);

        server.updateView(set(node));

        serverStates.put
          (node, new ServerState(server, Servers.bridge(localServer, server)));
      }
    }
  }

  private static class ServerState {
    public final NetworkServer server;
    public final Subscription subscription;

    public ServerState(NetworkServer server, Subscription subscription) {
      this.server = server;
      this.subscription = subscription;
    }
  }
}
