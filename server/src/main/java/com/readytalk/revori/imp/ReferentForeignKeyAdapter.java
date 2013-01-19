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

import java.util.ArrayList;
import java.util.List;

import com.readytalk.revori.Column;
import com.readytalk.revori.Expression;
import com.readytalk.revori.ForeignKey;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.TableReference;

public class ReferentForeignKeyAdapter {
  public final ForeignKey constraint;
  public final QueryTemplate refererQuery;
  public final QueryTemplate referentQuery;

  public ReferentForeignKeyAdapter(ForeignKey constraint) {
    this.constraint = constraint;

    TableReference referer = reference(constraint.refererTable);

    Expression refererTest = constant(true);

    for (Column<?> c: constraint.refererColumns) {
      refererTest = and
        (refererTest, equal(reference(referer, c), parameter()));
    }
    
    List<Column<?>> refererColumns = referer.table.primaryKey.columns;
    List<Expression> refererColumnReferences
      = new ArrayList<Expression>(refererColumns.size());

    for (Column<?> c: refererColumns) {
      refererColumnReferences.add(reference(referer, c));
    }

    refererQuery = new QueryTemplate
      (refererColumnReferences, referer, refererTest);
    
    TableReference referent = reference(constraint.referentTable);

    Expression referentTest = constant(true);

    for (Column<?> c: constraint.referentColumns) {
      referentTest = and
        (referentTest, equal(reference(referent, c), parameter()));
    }
    
    List<Column<?>> referentColumns = referent.table.primaryKey.columns;
    List<Expression> referentColumnReferences
      = new ArrayList<Expression>(refererColumns.size());

    for (Column<?> c: referentColumns) {
      referentColumnReferences.add(reference(referent, c));
    }

    referentQuery = new QueryTemplate
      (referentColumnReferences, referent, referentTest);
  }

  private Object[] parameters(List<Column<?>> columns, Node tree) {
    Object[] parameters = new Object[columns.size()];
    for (int i = 0; i < parameters.length; ++i) {
      Node n = Node.find(tree, columns.get(i), Compare.ColumnComparator);
      parameters[i] = n == Node.Null ? null : n.value;
    }
    return parameters;
  }

  private QueryResult query(QueryTemplate query,
                            Revision revision, List<Column<?>> columns, Node tree)
  {
    return DefaultRevision.Empty.diff(revision, query, parameters(columns, tree));
  }

  public void visitBrokenReferences(Revision revision, Node tree,
                                    Visitor visitor)
  {
    if (query(referentQuery, revision, constraint.referentColumns, tree)
        .nextRow() == QueryResult.Type.End)
    {
      QueryResult result = query
        (refererQuery, revision, constraint.referentColumns, tree);
      
      Object[] row = new Object
        [constraint.refererTable.primaryKey.columns.size()];

      while (true) {
        QueryResult.Type type = result.nextRow();
        switch (type) {
        case End:
          return;

        case Inserted:
          for (int i = 0; i < row.length; ++i) {
            row[i] = result.nextItem();
          }
          visitor.visit(row);
          break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
    }
  }

  public interface Visitor {
    public void visit(Object[] row);
  }
}
