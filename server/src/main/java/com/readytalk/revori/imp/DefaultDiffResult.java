/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.readytalk.revori.Column;
import com.readytalk.revori.DiffResult;
import com.readytalk.revori.Index;
import com.readytalk.revori.Table;
import com.readytalk.revori.imp.DiffIterator.DiffPair;

class DefaultDiffResult implements DiffResult {
  private enum State {
    Flush, FlushKey() {
      public Object fork(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].fork;
        return n == null ? null : n.key;
      }

      public Object base(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].base;
        return n == null ? null : n.key;
      }

      public Node forkTree(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].fork;
        return n == null ? Node.Null : (Node) n.value;
      }

      public Node baseTree(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].base;
        return n == null ? Node.Null : (Node) n.value;
      }

      public void skip(DefaultDiffResult r) {
        int clientDepth = r.clientDepth;
        while (r.depth > clientDepth) {
          r.ascend();
        }
        r.clientDepth = r.depth;
        r.state = Iterate;
      }  
    }, Descend, Ascend, Key, Value, PostValue() {
      public Object fork(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].fork;
        return n == null ? null : n.value;
      }

      public Object base(DefaultDiffResult r) {
        Node n = r.pairs[r.clientDepth].base;
        return n == null ? null : n.value;
      }
    }, Iterate, End;

    public Object fork(DefaultDiffResult r) {
      throw new IllegalStateException();
    }

    public Object base(DefaultDiffResult r) {
      throw new IllegalStateException();
    }

    public Node forkTree(DefaultDiffResult r) {
      throw new IllegalStateException();
    }

    public Node baseTree(DefaultDiffResult r) {
      throw new IllegalStateException();
    }

    public void skip(DefaultDiffResult r) {
      throw new IllegalStateException();
    }
  }

  private final DiffIterator[] iterators = new DiffIterator[Constants.MaxDepth];
  private final DiffPair[] pairs = new DiffPair[Constants.MaxDepth];
  private final boolean[] clientHasKey = new boolean[Constants.MaxDepth];
  private final DefaultRevision fork;
  private final boolean skipBrokenReferences;
  private State state = State.Iterate;
  private State nextState;
  private NodeStack baseStack;
  private NodeStack forkStack;
  private Table table;
  private int depth;
  private int bottom;
  private int clientDepth;
  private Set<Column<?>> primaryKey;
  private List<RefererForeignKeyAdapter> refererKeyAdapters;

  public DefaultDiffResult(DefaultRevision base,
                      NodeStack baseStack,
                      DefaultRevision fork,
                      NodeStack forkStack,
                      boolean skipBrokenReferences)
  {
    iterators[0] = new DiffIterator
      (base.root,
       this.baseStack = new NodeStack(baseStack),
       fork.root,
       this.forkStack = new NodeStack(forkStack),
       Lists.newArrayList(Interval.Unbounded).iterator(),
       false, Compare.TableComparator);

    pairs[0] = new DiffPair();

    this.fork = fork;

    this.skipBrokenReferences = skipBrokenReferences;
  }

  public DiffResult.Type next() {
    while (true) {
      switch (state) {
      case Flush:
        if (! clientHasKey[clientDepth]) {
          state = State.FlushKey; 
          clientHasKey[clientDepth] = true;
          return DiffResult.Type.Key;
        } else if (clientDepth != depth) {
          if (clientDepth == Constants.TableDataDepth) {
            clientDepth += 2;
          } else {
            ++ clientDepth;
          }

          return DiffResult.Type.Descend;
        } else {
          state = nextState;
          nextState = null;
        }
        break;

      case FlushKey:
        state = State.Flush;
        break;

      case Descend:
        descend();
        state = State.Iterate;
        break;

      case Ascend:
        ascend();
        state = State.Iterate;
        break;

      case Key: {
        DiffPair pair = pairs[depth];

        if (pair.base != null && pair.fork != null) {
          if (depth > Constants.IndexDataDepth && depth == bottom) {
            if (Compare.equal(pair.base.value, pair.fork.value)) {
              state = State.Iterate;
            } else {
              nextState = State.Value;
              state = State.Flush;
            }
          } else {
            state = State.Descend;
          }
        } else {
          if (depth > Constants.IndexDataDepth) {
            if (depth == bottom) {
              Object key = pair.base == null ? pair.fork.key : pair.base.key;

              if (primaryKey.contains(key)) {
                // no need to report the addition/subtraction of primary
                // key columns, since we've already covered them during
                // the descent
                state = State.Iterate;
              } else {
                nextState = State.Value;
                state = State.Flush;
              }
            } else if (depth == bottom - 1
                       && skipBrokenReferences
                       && pair.fork == null
                       && findBrokenReference
                       (fork, (Node) pair.base.value, refererKeyAdapters))
            {
              // no need to explicitly report deletion of rows which
              // cannot exist due to a foreign key constraint
              state = State.Iterate;              
            } else {
              nextState = State.Descend;
              state = State.Flush;
            }
          } else {
            nextState = State.Descend;
            state = State.Flush;
          }
        }
      } break;

      case Value:
        state = State.PostValue;
        return DiffResult.Type.Value;

      case PostValue:
        state = State.Iterate;
        break;

      case Iterate: {
        clientHasKey[depth] = false;
        DiffPair pair = pairs[depth];
        if (iterators[depth].next(pair)) {
          if (depth == Constants.TableDataDepth) {
            table = (Table)
              (pair.base == null ? pair.fork.key : pair.base.key);

            if (skipBrokenReferences) {
              refererKeyAdapters
                = ForeignKeys.getRefererForeignKeyAdapters
                (table, iterators[0].forkRoot, baseStack);
            }
          } else if (depth == Constants.IndexDataDepth) {
            Index index = (Index)
              (pair.base == null ? pair.fork.key : pair.base.key);

            if (Compare.equal
                (index, table.primaryKey, Compare.IndexComparator))
            {
              bottom = index.columns.size() + Constants.IndexDataBodyDepth;
              primaryKey = new TreeSet<Column<?>>(table.primaryKey.columns);
              descend();
            }
            break;
          }

          state = State.Key;
        } else if (depth == 0) {
          state = State.End;
        } else {
          state = State.Ascend;
          if (clientDepth == depth) {
            if (depth == Constants.IndexDataDepth + 1) {
              clientDepth -= 2;
            } else {
              -- clientDepth;
            }
            return DiffResult.Type.Ascend;
          }
        }
      } break;

      case End:
        // todo: be defensive to ensure we can safely keep returning
        // DiffResult.Type.End if the application calls next again
        // after this.  The popStack calls below should not be called
        // more than once.

        baseStack.popStack();
        forkStack.popStack();
        
        baseStack = null; //for safety
        forkStack = null; // ditto

        return DiffResult.Type.End;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }
  }

  public void descend() {
    DiffPair pair = pairs[depth];
    Node base = pair.base;
    Node fork = pair.fork;

    ++ depth;

    iterators[depth] = new DiffIterator
      (base == null ? Node.Null : (Node) base.value,
       baseStack = new NodeStack(baseStack),
       fork == null ? Node.Null : (Node) fork.value,
       forkStack = new NodeStack(forkStack),
       Lists.newArrayList(Interval.Unbounded).iterator(),
       false, depth == Constants.IndexDataDepth ? Compare.IndexComparator
       : (depth == bottom ? Compare.ColumnComparator
          : table.primaryKey.columns.get
          (depth - Constants.IndexDataBodyDepth).comparator));

    if (pairs[depth] == null) {
      pairs[depth] = new DiffPair();
    }
  }

  public void ascend() {
    iterators[depth] = null;

    -- depth;

    baseStack = baseStack.popStack();
    forkStack = forkStack.popStack();
  }

  public Object fork() {
    return state.fork(this);
  }

  public Object base() {
    return state.base(this);
  }

  public Node forkTree() {
    return state.forkTree(this);
  }

  public Node baseTree() {
    return state.baseTree(this);
  }

  public void skip() {
    state.skip(this);
  }

  private static boolean findBrokenReference
    (DefaultRevision revision,
     Node tree,
     List<RefererForeignKeyAdapter> adapters)
  {
    for (RefererForeignKeyAdapter adapter: adapters) {
      if (adapter.isBrokenReference(revision, tree)) {
        return true;
      }
    }
    return false;
  }
}
