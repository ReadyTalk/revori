/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.imp.Interval.BoundType;

import java.util.Comparator;
import java.util.Iterator;

class DiffIterator {
  private static final boolean Verbose = false;

  public final Node baseRoot;
  public final NodeStack base;
  public final Node forkRoot;
  public final NodeStack fork;
  public final Iterator<Interval> intervalIterator;
  public final boolean visitUnchanged;
  public final Comparator comparator;
  public Interval currentInterval;
  public boolean foundStart;

  public DiffIterator(Node baseRoot,
                      NodeStack base,
                      Node forkRoot,
                      NodeStack fork,
                      Iterator<Interval> intervalIterator,
                      boolean visitUnchanged,
                      Comparator comparator)
  {
    this.baseRoot = baseRoot;
    this.base = base;
    this.forkRoot = forkRoot;
    this.fork = fork;
    this.intervalIterator = intervalIterator;
    this.visitUnchanged = visitUnchanged;
    this.comparator = comparator;
    this.currentInterval = intervalIterator.next();
  }
  
  private static int compareForDescent(Node n,
                                       Object key,
                                       BoundType boundType,
                                       boolean high,
                                       Comparator comparator)
  {
    if (n == Node.Null) {
      return 0;
    }

    int difference = Compare.compare(n.key, key, boundType, high, comparator);
    if (difference > 0) {
      n = n.left;
      if (n == Node.Null) {
        return 0;
      } else if (Compare.compare(n.key, key, boundType, high, comparator) >= 0)
      {
        return 1;
      } else {
        while (n != Node.Null
               && Compare.compare(n.key, key, boundType, high, comparator) < 0)
        {
          n = n.right;
        }

        if (n == Node.Null) {
          // todo: cache this result so we don't have to loop every time
          return 0;
        } else {
          return 1;
        }
      }
    } else if (difference < 0) {
      n = n.right;
      if (n == Node.Null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      return 0;
    }
  }

  public boolean next(DiffPair pair) {
    if (currentInterval != null) {
      if (next(currentInterval, pair)) {
        return true;
      }
    }

    while (true) {
      if (intervalIterator.hasNext()) {
        foundStart = false;
        base.clear();
        fork.clear();
        currentInterval = intervalIterator.next();
        if (next(currentInterval, pair)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  private void findStart(Interval interval) {
    base.push(baseRoot);
    fork.push(forkRoot);

    while (true) {
      int baseDifference = compareForDescent
        (base.top, interval.low, interval.lowBoundType, false, comparator);

      int forkDifference = compareForDescent
        (fork.top, interval.low, interval.lowBoundType, false, comparator);

      if (baseDifference == 0) {
        if (forkDifference == 0) {
          break;
        } else {
          fork.descend(forkDifference);
        }
      } else if (forkDifference == 0) {
        base.descend(baseDifference);
      } else {
        int difference;
        if (base.top == fork.top) {
          if (visitUnchanged) {
            difference = 0;
          } else {
            if (baseDifference < 0) {
              base.clear();
              fork.clear();
            }
            break;
          }
        } else {
          difference = Compare.compare(base.top.key, fork.top.key, comparator);
        }

        if (difference > 0) {
          if (baseDifference > 0) {
            base.descend(baseDifference);
          } else {
            fork.descend(forkDifference);              
          }
        } else if (difference < 0) {
          if (forkDifference > 0) {
            fork.descend(forkDifference);
          } else {
            base.descend(baseDifference);              
          }
        } else {
          base.descend(baseDifference);
          fork.descend(forkDifference);            
        }
      }
    }
      
    if (base.top != null
        && (base.top == Node.Null || Compare.compare
            (base.top.key, interval.low, interval.lowBoundType, false,
             comparator) < 0))
    {
      base.clear();
    }

    if (Verbose) {
      System.out.println("base start from "
                         + (base.top == null ? "nothing" : base.top.key));
      Node.dump(baseRoot, System.out, 0);
    }

    if (fork.top != null
        && (fork.top == Node.Null || Compare.compare
            (fork.top.key, interval.low, interval.lowBoundType, false,
             comparator) < 0))
    {
      fork.clear();
    }

    if (Verbose) {
      System.out.println("fork start from "
                         + (fork.top == null ? "nothing" : fork.top.key));
      Node.dump(forkRoot, System.out, 0);
    }
  }

  private boolean next(Interval interval, DiffPair pair) {
    if (! foundStart) {
      findStart(interval);
        
      foundStart = true;
    }

    while (true) {
      int baseDifference = base.top == null ? 1 : Compare.compare
        (base.top.key, interval.high, interval.highBoundType, true,
         comparator);

      int forkDifference = fork.top == null ? 1 : Compare.compare
        (fork.top.key, interval.high, interval.highBoundType, true,
         comparator);
      
      if (baseDifference <= 0) {
        if (forkDifference <= 0) {
          int difference;
          if (base.top == fork.top) {
            if (visitUnchanged) {
              difference = 0;
            } else {
              base.ascendNext();
              fork.ascendNext();
              continue;
            }
          } else {
            difference = Compare.compare
              (base.top.key, fork.top.key, comparator);
          }

          if (difference > 0) {
            pair.base = null;
            pair.fork = fork.top;
            fork.next(); 
            return true;           
          } else if (difference < 0) {
            pair.base = base.top;
            pair.fork = null;
            base.next();
            return true;
          } else {
            pair.base = base.top;
            pair.fork = fork.top;
            base.next();
            fork.next();
            return true;
          }
        } else {
          pair.base = base.top;
          pair.fork = null;
          base.next();
          return true;
        }
      } else {
        pair.base = null;
        if (forkDifference <= 0) {
          pair.fork = fork.top;
          fork.next();
          return true;
        } else {
          pair.fork = null;
          return false;
        }
      }
    }
  }

  public static class DiffPair {
    public Node base;
    public Node fork;
  }
}
