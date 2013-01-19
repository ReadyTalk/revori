/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.ExpressionFactory.and;
import static com.readytalk.revori.ExpressionFactory.constant;
import static com.readytalk.revori.ExpressionFactory.equal;
import static com.readytalk.revori.ExpressionFactory.parameter;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.SourceFactory.reference;

import java.util.Collections;
import java.util.List;

import com.readytalk.revori.Column;
import com.readytalk.revori.Expression;
import com.readytalk.revori.ForeignKey;
import com.readytalk.revori.ForeignKeyException;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.TableReference;

public class RefererForeignKeyAdapter {
  public final ForeignKey constraint;
  public final QueryTemplate query;

  public RefererForeignKeyAdapter(ForeignKey constraint) {
    this.constraint = constraint;

    TableReference referent = reference(constraint.referentTable);

    Expression referentTest = constant(true);

    for (Column<?> c: constraint.referentColumns) {
      referentTest = and
        (referentTest, equal(reference(referent, c), parameter()));
    }

    query = new QueryTemplate
      (Collections.<Expression>emptyList(), referent,
       referentTest);
  }

  private Object[] parametersOrNull(List<Column<?>> columns, Node tree) {
    Object[] parameters = new Object[columns.size()];
    for (int i = 0; i < parameters.length; ++i) {
      Node n = Node.find(tree, columns.get(i), Compare.ColumnComparator);
      if(n == Node.Null) {
        return null;
      }
      parameters[i] = n.value;
    }
    return parameters;
  }

  private boolean queryEmptyAndNotNull(QueryTemplate query,
                            Revision revision, List<Column<?>> columns, Node tree)
  {
    Object[] params = parametersOrNull(columns, tree);
    if (params == null) {
      throw new NullPointerException();
    }

    return DefaultRevision.Empty.diff
      (revision, query, params).nextRow() == QueryResult.Type.End;
  }

  public void handleInsert(DefaultRevisionBuilder builder, Node tree) {
    if (queryEmptyAndNotNull(query, builder.result, constraint.refererColumns, tree)) {
      throw new ForeignKeyException();
    }
  }

  public boolean isBrokenReference(Revision revision, Node tree) {
    return queryEmptyAndNotNull(query, revision, constraint.refererColumns, tree);
  }
}
