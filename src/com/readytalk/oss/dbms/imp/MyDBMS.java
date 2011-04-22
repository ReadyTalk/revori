package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.copy;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DiffResult;
import com.readytalk.oss.dbms.ConflictResolver;

public class MyDBMS implements DBMS {
  public Revision revision() {
    return MyRevision.Empty;
  }

  public QueryResult diff(Revision base,
                          Revision fork,
                          QueryTemplate template,
                          Object ... parameters)
  {
    MyRevision myBase;
    MyRevision myFork;
    try {
      myBase = (MyRevision) base;
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

    return new MyQueryResult(myBase, myFork, template, copy(parameters));
  }

  public DiffResult diff(Revision base,
                         Revision fork)
  {
    MyRevision myBase;
    MyRevision myFork;
    try {
      myBase = (MyRevision) base;
      myFork = (MyRevision) fork;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    return new MyDiffResult(myBase, new NodeStack(), myFork, new NodeStack());
  }

  public RevisionBuilder builder(Revision base) {
    MyRevision myBase;
    try {
      myBase = (MyRevision) base;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return new MyRevisionBuilder(new Object(), myBase, new NodeStack());
  }

  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver)
  {
    MyRevision myBase;
    MyRevision myLeft;
    MyRevision myRight;
    try {
      myBase = (MyRevision) base;
      myLeft = (MyRevision) left;
      myRight = (MyRevision) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return Merge.mergeRevisions(myBase, myLeft, myRight, conflictResolver);
  }
}
