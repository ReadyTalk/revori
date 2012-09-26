package com.readytalk.revori.subscribe;

import java.util.HashMap;
import java.util.Map;

import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.Source;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.SourceVisitor;
import com.readytalk.revori.server.RevisionServer;

import com.readytalk.revori.util.SetMultimap;

public class DiffMachine {

  private static boolean DebugThreads = true;

  private final SetMultimap<Table, Matcher> newMatchers = new SetMultimap<Table, Matcher>();
  
  private final SetMultimap<Table, Matcher> matchersForTable = new SetMultimap<Table, Matcher>();

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

  private static void register(final Matcher matcher, final SetMultimap<Table, Matcher> matchers) {
    matcher.query.source.visit(new SourceVisitor() {
      public void visit(Source s) {
        if(s instanceof TableReference) {
          matchers.put(((TableReference) s).table, matcher);
        }
      }
    });
  }

  public static void unregister(final Matcher matcher,
                                final SetMultimap<Table, Matcher> matchers)
  {
    matcher.query.source.visit(new SourceVisitor() {
      public void visit(Source s) {
        if (s instanceof TableReference) {
          matchers.remove(((TableReference) s).table, matcher);
        }
      }
    });
  }

  public Subscription subscribe(RowListener listener, QueryTemplate query, Object... params) {
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

  private void promoteMatchers() {
    matchersForTable.putAll(newMatchers);
    newMatchers.clear();
  }

  public boolean next() {
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
          iterator = new DiffIterator
            (Revisions.Empty, base.revision, newMatchers);
          
          state = State.New;
        } else {
          head = server.next(base);
  
          if (head == null) {
            return false;
          }
  
          iterator = new DiffIterator
            (base.revision, head.revision, matchersForTable);
  
          state = State.Uncached;
        }
      } break;
      
      case New: {
        if(iterator.next()) {
          return true;
        } else {
          promoteMatchers();
          state = State.Start;
        }
      } break;

      case Uncached: {
        if(iterator.next()) {
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


}
