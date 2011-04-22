package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;

import java.util.List;

class MyRevision implements Revision {
  public static final MyRevision Empty = new MyRevision
    (new Object(), Node.Null);

  public final Object token;
  public Node root;

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

}
