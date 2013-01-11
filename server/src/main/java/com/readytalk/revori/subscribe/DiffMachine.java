package com.readytalk.revori.subscribe;

import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Source;
import com.readytalk.revori.SourceVisitor;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.server.RevisionServer;
public class DiffMachine<Context> {

  private static boolean DebugThreads = true;

  private final SetMultimap<Table, Matcher<Context>> newMatchers = HashMultimap.create();
  
  private final SetMultimap<Table, Matcher<Context>> matchersForTable = HashMultimap.create();

  private LinearRevision base;
  private LinearRevision head;

  private DiffServer server;

  private DiffIterator iterator;

  private State state = State.Start;

  private final boolean autoDeliver;

  private Thread thread;

  private enum State { Start, New, Cached, Uncached; };

  public DiffMachine(DiffServer server) {
    this(server, true);
  }

  public DiffMachine(DiffServer server, boolean autoDeliver) {
    this.server = server;
    this.base = server.tail();

    // TODO: perhaps move this autoDeliver into a wrapper object?
    this.autoDeliver = autoDeliver;
    if(autoDeliver) {
      server.register(new Runnable() {
        public void run() {
          while(next()) {}
        }
      });
    }
  }

  public DiffMachine(RevisionServer server, boolean autoDeliver) {
    this(new DiffServer(server), autoDeliver);
  }

  private static <Context> void register
    (final Matcher<Context> matcher,
     final SetMultimap<Table, Matcher<Context>> matchers)
  {
    matcher.query.source.visit(new SourceVisitor() {
      public void visit(Source s) {
        if(s instanceof TableReference) {
          matchers.put(((TableReference) s).table, matcher);
        }
      }
    });
  }

  private static <Context> void unregister
    (final Matcher<Context> matcher,
     final SetMultimap<Table, Matcher<Context>> matchers)
  {
    matcher.query.source.visit(new SourceVisitor() {
      public void visit(Source s) {
        if (s instanceof TableReference) {
          matchers.remove(((TableReference) s).table, matcher);
        }
      }
    });
  }

  public Subscription subscribe(ContextRowListener<Context> listener,
                                QueryTemplate query,
                                Object... params)
  {
    final Matcher matcher = new Matcher(listener, query, params);
    register(matcher, newMatchers);

    if(autoDeliver) {
      while(next()) {}
    }

    return new Subscription() {
      boolean subscribed = true;
      public void cancel() {
        if(subscribed) {
          subscribed = false;
          unregister(matcher, newMatchers);
          unregister(matcher, matchersForTable);
        }
      }
    };
  }

  public Subscription subscribe(RowListener listener,
                                QueryTemplate query,
                                Object... params)
  {
    return subscribe(new ContextRowAdapter(listener), query, params);
  }

  private void promoteMatchers() {
    matchersForTable.putAll(newMatchers);
    newMatchers.clear();
  }

  public boolean next() {
    return next(null);
  }

  public boolean next(@Nullable Context context) {
    if (DebugThreads) {
      if (thread == null) {
        thread = Thread.currentThread();
      } else if (thread != Thread.currentThread()) {
        throw new IllegalStateException
          ("expected " + thread + " got " + Thread.currentThread());
      }
    }

    while (true) {
      switch (state) {
      case Start: {
        if(newMatchers.size() > 0) {
          iterator = new DiffIterator<Context>
            (Revisions.Empty, base.revision, newMatchers);
          
          state = State.New;
        } else {
          head = server.next(base);
  
          if (head == null) {
            return false;
          }
  
          iterator = new DiffIterator<Context>
            (base.revision, head.revision, matchersForTable);
  
          state = State.Uncached;
        }
      } break;
      
      case New: {
        if(iterator.next(context)) {
          return true;
        } else {
          promoteMatchers();
          state = State.Start;
        }
      } break;

      case Uncached: {
        if(iterator.next(context)) {
          return true;
        } else {
          base = head;
          state = State.Start;
        }
      } break;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }
  }

  private static class ContextRowAdapter<Context>
    implements ContextRowListener<Context>
  {
    private final RowListener listener;

    public ContextRowAdapter(RowListener listener) {
      this.listener = listener;
    }

    public void handleUpdate(Context context, Object[] row) {
      listener.handleUpdate(row);
    }
    
    public void handleDelete(Context context, Object[] row) {
      listener.handleDelete(row);
    }
  
    public boolean equals(Object o) {
      return o instanceof ContextRowAdapter
        && ((ContextRowAdapter) o).listener.equals(listener);
    }
  }
}
