package com.readytalk.revori.subscribe;

import java.util.HashMap;
import java.util.Map;

import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.Source;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.SourceVisitor;

import com.readytalk.revori.util.SetMultimap;

public class DiffMachine {

  // used only for registering / unregistering matchers
  private final Map<RowListener, Matcher> matchers = new HashMap<RowListener, Matcher>();

  private final SetMultimap<Table, Matcher> newMatchers = new SetMultimap<Table, Matcher>();
  
  private final SetMultimap<Table, Matcher> matchersForTable = new SetMultimap<Table, Matcher>();

  private LinearRevision base;
  private LinearRevision head;

  private DiffServer server;

  private DiffIterator iterator;

  private State state = State.Start;

  private enum State { Start, New, Cached, Uncached; };

  public DiffMachine(DiffServer server) {
    this.server = server;
    this.base = server.tail();
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

  public void register(RowListener listener, QueryTemplate query, Object... params) {
    final Matcher matcher = new Matcher(listener, query, params);
    matchers.put(listener, matcher);
    register(matcher, newMatchers);
  }

  public static void unregister(final Matcher matcher,
                                final SetMultimap<Table, Matcher> matchers)
  {
    matcher.query.source.visit(new SourceVisitor() {
      public void visit(Source s) {
        if (s instanceof TableReference) {
          matchers.get(((TableReference) s).table).remove(matcher);
        }
      }
    });
  }

  public void unregister(RowListener listener) {
    final Matcher matcher = matchers.remove(listener);

    if (matcher != null) {
      unregister(matcher, newMatchers);
      unregister(matcher, matchersForTable);
    }
  }

  public void promoteMatchers() {
    matchersForTable.putAll(newMatchers);
    newMatchers.clear();
  }

  public boolean next() {
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