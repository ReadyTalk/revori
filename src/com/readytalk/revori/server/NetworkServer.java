/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import com.readytalk.revori.Table;
import com.readytalk.revori.Column;
import com.readytalk.revori.server.protocol.Readable;
import com.readytalk.revori.server.protocol.Writable;
import com.readytalk.revori.server.protocol.Stringable;

import java.util.Set;

public interface NetworkServer extends RevisionServer {
  public void accept(NodeID source, Readable message);

  public void updateView(Set<NodeID> directlyConnectedNodes);

  public static class NodeID implements Comparable<NodeID>, Stringable {
    public final String id;

    public NodeID(String id) {
      this.id = id;
    }

    public int hashCode() {
      return id.hashCode();
    }

    public boolean equals(Object o) {
      return o instanceof NodeID && compareTo((NodeID) o) == 0;
    }

    @Override
    public int compareTo(NodeID o) {
      return id.compareTo(o.id);
    }

    public String toString() {
      return "nodeID[" + id + "]";
    }

    public String asString() {
      return id;
    }
  }

  public static interface NodeConflictResolver {
    public Object resolveConflict(NodeID leftNode,
                                  NodeID rightNode,
                                  Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue);
  }

  public static final NodeConflictResolver NodeConflictRestrict
    = new NodeConflictResolver() {
        public Object resolveConflict(NodeID leftNode,
                                      NodeID rightNode,
                                      com.readytalk.revori.Table table,
                                      Column column,
                                      Object[] primaryKeyValues,
                                      Object baseValue,
                                      Object leftValue,
                                      Object rightValue)
        {
          throw new RuntimeException();
        }
      };

  public static interface Network {
    public void send(NodeID source, NodeID destination, Writable message);
  }
}
