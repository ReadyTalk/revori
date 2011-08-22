package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.copy;


import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DiffResult;
import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.ForeignKeyResolver;

import java.util.List;

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

    List<Column> columns = index.columns;

    if (pathLength != columns.size() + 2) {
      throw new IllegalArgumentException
        ("wrong number of parameters for specified index");
    }

    Column column;
    try {
      column = (Column) path[pathOffset + columns.size() + 1];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expected column as second-to-last element of path");        
    }

    Comparable[] myPath = new Comparable[columns.size() + 2];
    myPath[0] = index.table;
    myPath[1] = index;
    for (int i = 0; i < columns.size(); ++i) {
      myPath[i + 2] = (Comparable) path[pathOffset + i + 1];
    }

    Node n = Node.find(Node.pathFind(root, myPath), column);
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
    MyRevision myBase = this;
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

    return new MyQueryResult
      (myBase, null, myFork, null, template, copy(parameters));
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
}
