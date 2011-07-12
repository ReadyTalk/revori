package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.copy;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DiffResult;
import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.ForeignKeyResolver;

public class MyDBMS implements DBMS {
  public Revision revision() {
    return MyRevision.Empty;
  }

  public QueryResult diff(Revision base,
                          Revision fork,
                          QueryTemplate template,
                          Object ... parameters)
  {
    return base.diff(fork, template, parameters);
  }

  public DiffResult diff(Revision base,
                         Revision fork)
  {
    return base.diff(fork, false);
  }

  public RevisionBuilder builder(Revision base) {
    return base.builder();
  }

  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver,
                        ForeignKeyResolver foreignKeyResolver)
  {
    return base.merge(left, right, conflictResolver, foreignKeyResolver);
  }
}
