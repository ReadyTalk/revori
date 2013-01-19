/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.google.common.base.Preconditions.checkArgument;

import com.readytalk.revori.Join;
import com.readytalk.revori.QueryResult;

public class JoinIterator implements SourceIterator {
  private final JoinAdapter join;
  private final DefaultRevision base;
  private final DefaultRevision fork;
  public final ExpressionAdapter test;
  private final ExpressionContext expressionContext;
  private final boolean visitUnchanged;
  private final SourceIterator leftIterator;
  private NodeStack rightBaseStack;
  private NodeStack rightForkStack;
  private QueryResult.Type leftType;
  private SourceIterator rightIterator;
  private boolean sawRightUnchanged;
  private boolean sawRightInsert;
  private boolean sawRightDelete;
  private boolean sawRightEnd;
  private boolean setUndefinedReferences;

  public JoinIterator(JoinAdapter join,
                      DefaultRevision base,
                      NodeStack baseStack,
                      DefaultRevision fork,
                      NodeStack forkStack,
                      ExpressionAdapter test,
                      ExpressionContext expressionContext,
                      boolean visitUnchanged)
  {
    this.join = join;
    this.base = base;
    this.fork = fork;
    this.test = test;
    this.expressionContext = expressionContext;
    this.visitUnchanged = visitUnchanged;
    this.leftIterator = join.left.iterator
      (base, baseStack, fork, forkStack, test, expressionContext, true);
  }

  private void setUndefinedReferences() {
    setUndefinedReferences = true;

    join.right.visit
      (expressionContext, new ColumnReferenceAdapterVisitor() {
          public void visit(ColumnReferenceAdapter r) {
            r.value = Compare.Dummy;
          }
        });
  }

  public QueryResult.Type nextRow() {
    while (true) {
      if (sawRightEnd) {
        if (setUndefinedReferences) {
          setUndefinedReferences = false;
          join.right.visit
            (expressionContext, new ColumnReferenceAdapterVisitor() {
                public void visit(ColumnReferenceAdapter r) {
                  r.value = Compare.Undefined;
                }
              });
        }
        sawRightUnchanged = false;
        sawRightInsert = false;
        sawRightDelete = false;
        sawRightEnd = false;
        rightIterator = null;
      }

      if (rightIterator == null) {
        leftType = leftIterator.nextRow();
        switch (leftType) {
        case End:
          return QueryResult.Type.End;

        case Unchanged:
          if (rightBaseStack == null) {
            rightBaseStack = new NodeStack();
          }

          if (rightForkStack == null) {
            rightForkStack = new NodeStack();
          }

          rightIterator = join.right.iterator
            (base, rightBaseStack, fork, rightForkStack, test,
             expressionContext,
             visitUnchanged || join.type == Join.Type.LeftOuter);
          break;

        case Inserted:
          if (rightForkStack == null) {
            rightForkStack = new NodeStack();
          }

          rightIterator = join.right.iterator
            (DefaultRevision.Empty, NodeStack.Null, fork, rightForkStack, test,
             expressionContext, true);
          break;

        case Deleted:
          if (rightForkStack == null) {
            rightForkStack = new NodeStack();
          }

          rightIterator = join.right.iterator
            (DefaultRevision.Empty, NodeStack.Null, base, rightForkStack, test,
             expressionContext, true);
          break;

        default: throw new RuntimeException
            ("unexpected result type: " + leftType);
        }
      }

      QueryResult.Type rightType = rightIterator.nextRow();
      switch (rightType) {
      case End:
        sawRightEnd = true;
        if (join.type == Join.Type.LeftOuter) {
          switch (leftType) {
          case Unchanged:
            if (sawRightInsert) {
              if (! (sawRightDelete || sawRightUnchanged)) {
                setUndefinedReferences();
                return QueryResult.Type.Deleted;
              }
            } else if (sawRightDelete && ! sawRightUnchanged) {
              setUndefinedReferences();
              return QueryResult.Type.Inserted;
            }
            break;

          case Inserted:
            if (! sawRightInsert) {
              setUndefinedReferences();
              return QueryResult.Type.Inserted;
            }
            break;
              
          case Deleted:
            if (! sawRightInsert) {
              setUndefinedReferences();
              return QueryResult.Type.Deleted;
            }
            break;

          default: throw new RuntimeException
              ("unexpected result type: " + leftType);
          }
        }
        break;

      case Unchanged:
        sawRightUnchanged = true;
        checkArgument(leftType == QueryResult.Type.Unchanged);
        if (visitUnchanged) {
          return QueryResult.Type.Unchanged;
        }
        break;

      case Inserted:
        sawRightInsert = true;
        switch (leftType) {
        case Unchanged:
        case Inserted:
          return QueryResult.Type.Inserted;

        case Deleted:
          return QueryResult.Type.Deleted;

        default: throw new RuntimeException
            ("unexpected result type: " + leftType);
        }

      case Deleted:
        sawRightDelete = true;
        checkArgument(leftType == QueryResult.Type.Unchanged);
        return QueryResult.Type.Deleted;

      default: throw new RuntimeException
          ("unexpected result type: " + rightType);
      }
    }
  }

  public boolean rowUpdated() {
    return false;
  }
}
