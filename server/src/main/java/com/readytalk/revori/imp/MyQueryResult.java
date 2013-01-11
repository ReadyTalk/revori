/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.readytalk.revori.Expression;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;

class MyQueryResult implements QueryResult {
  private static class ChangeFinder implements SourceAdapterVisitor {
    public final MyRevision base;
    public final MyRevision fork;
    public boolean foundChanged;

    public ChangeFinder(MyRevision base, MyRevision fork) {
      this.base = base;
      this.fork = fork;
    }

    public void visit(SourceAdapter adapter) {
      if (adapter instanceof TableAdapter) {
        TableAdapter tableAdapter = (TableAdapter) adapter;
        if (Node.find(base.root, tableAdapter.tableReference.table,
                      Compare.TableComparator)
            != Node.find(fork.root, tableAdapter.tableReference.table,
                         Compare.TableComparator))
        {
          foundChanged = true;
        }
      }
    }
  }

  public final SourceAdapter source;
  public final List<ExpressionAdapter> expressions;
  public final ExpressionContext expressionContext;
  public final ExpressionAdapter test;
  public final MyRevision base;
  public final MyRevision fork;
  public final NodeStack baseStack;
  public final NodeStack forkStack;
  public SourceIterator iterator;
  public int nextItemIndex;

  public MyQueryResult(MyRevision base,
                       @Nullable NodeStack baseStack,
                       MyRevision fork,
                       @Nullable NodeStack forkStack,
                       QueryTemplate template,
                       Object[] parameters)
  {
    this(base, baseStack, fork, forkStack, template, parameters, false);
  }

  public MyQueryResult(MyRevision base,
                       @Nullable NodeStack baseStack,
                       MyRevision fork,
                       @Nullable NodeStack forkStack,
                       QueryTemplate template,
                       Object[] parameters,
                       boolean force)
  {
    if (base == fork && (! force)) {
      source = null;
      expressions = null;
      expressionContext = null;
      test = null;
    } else {
      ChangeFinder finder = new ChangeFinder(base, fork);
      SourceAdapter source = SourceAdapterFactory.makeAdapter(template.source);
      source.visit(finder);

      if (finder.foundChanged || force) {
        expressions = new ArrayList<ExpressionAdapter>
          (template.expressions.size());

        expressionContext = new ExpressionContext
          (parameters, force ? null : expressions);

        for (Expression e: template.expressions) {
          expressions.add
            (ExpressionAdapterFactory.makeAdapter(expressionContext, e));
        }

        if (baseStack == null) baseStack = new NodeStack();
        if (forkStack == null) forkStack = new NodeStack();

        test = ExpressionAdapterFactory.makeAdapter
          (expressionContext, template.test);

        this.source = source;
      } else {
        this.source = null;
        expressions = null;
        expressionContext = null;
        test = null;
      }
    }

    this.base = base;
    this.fork = fork;
    this.baseStack = baseStack;
    this.forkStack = forkStack;

    reset();
  }

  public void reset() {
    if (source != null) {
      iterator = source.iterator
        (base, baseStack, fork, forkStack, test, expressionContext, false);
    }
  }

  public QueryResult.Type nextRow() {
    if (iterator == null) {
      return QueryResult.Type.End;
    } else {
      nextItemIndex = 0;
      return iterator.nextRow();
    }
  }

  public Object nextItem() {
    if (iterator == null || nextItemIndex >= expressions.size()) {
      throw new NoSuchElementException();
    } else {
      return expressions.get(nextItemIndex++).evaluate(true);
    }      
  }

  public boolean rowUpdated() {
    return iterator != null && iterator.rowUpdated();
  }
}
