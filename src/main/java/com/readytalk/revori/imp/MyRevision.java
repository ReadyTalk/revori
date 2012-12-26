/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.copy;
import static com.readytalk.revori.util.Util.list;

import com.readytalk.revori.Index;
import com.readytalk.revori.View;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.DiffResult;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.Constant;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Comparators;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MyRevision implements Revision {
  public static final MyRevision Empty = new MyRevision
    (new Object(), Node.Null);

  public final Object token;
  public Node root;

  public static Revision empty() {
    return Empty;
  }

  public MyRevision(Object token, Node root) {
    this.token = token;
    this.root = root;
  }

  public <T> T query(Column<T> column, Index index, Object ... indexValues) {
    Object[] path = new Object[indexValues.length + 2];
    path[0] = index;
    System.arraycopy(indexValues, 0, path, 1, indexValues.length);
    path[indexValues.length + 1] = column;
    return (T) query(path);
  }

  public <T> Iterator<T> queryAll(final Column<T> column,
                                  final Index index,
                                  Object ... indexValues)
  {
    TableReference reference = new TableReference(index.table);

    final ColumnReferenceAdapter adapter = new ColumnReferenceAdapter
      (reference, column);

    Plan plan = new Plan(index);
    for (int i = 0; i < plan.scans.length; ++i) {
      if (i < indexValues.length) {
        ExpressionAdapter ea = new ConstantAdapter(indexValues[i]);
        plan.scans[i] = new IntervalScan(ea, ea);
      } else {
        plan.scans[i] = IntervalScan.Unbounded;
      }
    }

    ExpressionContext context = new ExpressionContext
      (new Object[0], list((ExpressionAdapter) adapter));

    context.columnReferences.add(adapter);

    final TableIterator it = new TableIterator
      (reference,
       MyRevision.Empty, NodeStack.Null,
       this, new NodeStack(),
       ConstantAdapter.True,
       context,
       plan,
       false);

    return new Iterator<T>() {
      QueryResult.Type type;

      public boolean hasNext() {
        if (type == null) {
          type = it.nextRow();
        }

        return type != QueryResult.Type.End;
      }

      public T next() {
        if (hasNext()) {
          T v = (T) adapter.evaluate(true);
          type = null;
          return v;
        } else {
          throw new NoSuchElementException();
        }
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public Object query(Object[] path,
                      int pathOffset,
                      int pathLength)
  {
    Index index;
    try {
      index = (Index) path[pathOffset];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expected index as first element of path");        
    }

    List<Column<?>> columns = index.columns;

    if (pathLength != columns.size() + 2) {
      throw new IllegalArgumentException
        ("wrong number of parameters for specified index");
    }

    Column<?> column;
    try {
      column = (Column<?>) path[pathOffset + columns.size() + 1];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expected column as second-to-last element of path");        
    }

    Object[] myPath = new Object[(columns.size() + 2) * 2];
    myPath[0] = index.table;
    myPath[1] = Compare.TableComparator;
    myPath[2] = index;
    myPath[3] = Compare.IndexComparator;
    for (int i = 0; i < columns.size(); ++i) {
      myPath[(i + 2) * 2] = path[pathOffset + i + 1];
      myPath[((i + 2) * 2) + 1] = columns.get(i).comparator;
    }

    Node n = Node.find
      (Node.pathFind(root, myPath), column, Compare.ColumnComparator);

    if (n == Node.Null) {
      return null;
    } else {
      return n.value;
    }
  }

  public Object query(Object ... path)
  {
    return query(path, 0, path.length);
  }

  public QueryResult diff(Revision fork,
                          QueryTemplate template,
                          Object ... parameters)
  {
    MyRevision myFork;
    try {
      myFork = (MyRevision) fork;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    if (parameters.length != template.parameterCount) {
      throw new IllegalArgumentException
        ("wrong number of parameters (expected "
         + template.parameterCount + "; got "
         + parameters.length + ")");
    }

    if (template.hasAggregates || (! template.orderByExpressions.isEmpty())) {
      // todo: look for an index or view which will allow us to
      // fulfill this query without creating a temporary view;
      // i.e. teach the query planner about order-by clauses,
      // aggregates, and views, so we only fall back to using a
      // temporary view when an exisiting view or index does not
      // suffice.  The query planner may be able to use a view even
      // for queries which do not involve aggregates or order-by
      // clauses.  Ultimately, it may help to remove the distinction
      // between indexes and views.

      View view = new View(template, parameters);
      MyRevisionBuilder builder = new MyRevisionBuilder
        (new Object(), myFork, new NodeStack());

      builder.addView(view, this);

      TableReference tableReference = new TableReference(view.table);
      List<Expression> expressions = new ArrayList(view.primaryKeyOffset);
      for (int i = 0; i < view.primaryKeyOffset; ++i) {
        expressions.add
          (new ColumnReference(tableReference, view.columns.get(i)));
      }

      return myFork.diff
        (builder.commit(), new QueryTemplate
         (expressions, tableReference, new Constant(true)));
    } else {
      return new MyQueryResult
        (this, null, myFork, null, template, copy(parameters));
    }
  }

  public DiffResult diff(Revision fork, boolean skipBrokenReferences)
  {
    MyRevision myBase = this;
    MyRevision myFork;
    try {
      myFork = (MyRevision) fork;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    return new MyDiffResult
      (myBase, new NodeStack(), myFork, new NodeStack(), skipBrokenReferences);
  }

  public RevisionBuilder builder() {
    return new MyRevisionBuilder(new Object(), this, new NodeStack());
  }

  public Revision merge(Revision left,
                        Revision right,
                        ConflictResolver conflictResolver,
                        ForeignKeyResolver foreignKeyResolver)
  {
    MyRevision myBase = this;
    MyRevision myLeft;
    MyRevision myRight;
    try {
      myLeft = (MyRevision) left;
      myRight = (MyRevision) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return Merge.mergeRevisions
      (myBase, myLeft, myRight, conflictResolver, foreignKeyResolver);
  }

  public String toString() {
    java.io.StringWriter sw = new java.io.StringWriter();
    java.io.PrintWriter pw = new java.io.PrintWriter(sw);
    for (NodeIterator it = new NodeIterator(new NodeStack(), root);
         it.hasNext();)
    {
      Node n = it.next();
      if (! (Constants.IndexTable.equals(n.key)
             || Constants.ViewTable.equals(n.key)
             || Constants.ForeignKeyTable.equals(n.key)))
      {
        Node.dump((Node) n.value, pw, 0);
      }
    }
    pw.flush();
    return sw.toString();
  }
  
  public boolean equals(Object other) {
    if(this == other) {
      return true;
    }
    return (other instanceof MyRevision)
      && diff((MyRevision)other, true).next() == DiffResult.Type.End;
  }
}
