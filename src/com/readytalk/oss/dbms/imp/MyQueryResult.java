package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Source;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Expression;

import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;

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
        if (Node.find(base.root, tableAdapter.tableReference.table)
            != Node.find(fork.root, tableAdapter.tableReference.table))
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
                       NodeStack baseStack,
                       MyRevision fork,
                       NodeStack forkStack,
                       QueryTemplate template,
                       Object[] parameters)
  {
    if (base == fork) {
      source = null;
      expressions = null;
      expressionContext = null;
      test = null;
    } else {
      ChangeFinder finder = new ChangeFinder(base, fork);
      SourceAdapter source = SourceAdapterFactory.makeAdapter(template.source);
      source.visit(finder);

      if (finder.foundChanged) {
        expressions = new ArrayList<ExpressionAdapter>
          (template.expressions.size());

        expressionContext = new ExpressionContext(parameters);

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
    if (iterator == null || nextItemIndex > expressions.size()) {
      throw new NoSuchElementException();
    } else {
      return expressions.get(nextItemIndex++).evaluate(true);
    }      
  }

  public boolean rowUpdated() {
    return iterator != null && iterator.rowUpdated();
  }
}
