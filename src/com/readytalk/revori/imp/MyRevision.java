package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.copy;

import com.readytalk.revori.Index;
import com.readytalk.revori.View;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
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
    Node.dump(root, pw, 0);
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
