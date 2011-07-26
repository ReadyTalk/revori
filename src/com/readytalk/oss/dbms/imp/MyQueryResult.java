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

  public final List<ExpressionAdapter> expressions;
  public final SourceIterator iterator;
  public int nextItemIndex;

  public MyQueryResult(MyRevision base,
                       MyRevision fork,
                       QueryTemplate template,
                       Object[] parameters)
  {
    if (base == fork) {
      expressions = null;
      iterator = null;
    } else {
      ChangeFinder finder = new ChangeFinder(base, fork);
      SourceAdapter source = SourceAdapterFactory.makeAdapter(template.source);
      source.visit(finder);

      if (finder.foundChanged) {
        expressions = new ArrayList<ExpressionAdapter>(template.expressions.size());

        ExpressionContext context = new ExpressionContext(parameters);

        for (Expression e: template.expressions) {
          expressions.add(ExpressionAdapterFactory.makeAdapter(context, e));
        }

        iterator = source.iterator
          (base, new NodeStack(), fork, new NodeStack(),
           ExpressionAdapterFactory.makeAdapter(context, template.test),
           context, false);
      } else {
        expressions = null;
        iterator = null;
      }
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
