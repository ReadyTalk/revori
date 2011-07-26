package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.SourceFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.constant;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.parameter;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.and;

import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyResolver;
import com.readytalk.oss.dbms.ForeignKeyException;

import java.util.List;
import java.util.ArrayList;

public class ReferentForeignKeyAdapter {
  public final ForeignKey constraint;
  public final QueryTemplate refererQuery;
  public final QueryTemplate referentQuery;

  public ReferentForeignKeyAdapter(ForeignKey constraint) {
    this.constraint = constraint;

    TableReference referer = reference(constraint.refererTable);

    Expression refererTest = constant(true);

    for (Column c: constraint.refererColumns) {
      refererTest = and
        (equal(reference(referer, c), parameter()), refererTest);
    }
    
    List<Column> refererColumns = referer.table.primaryKey.columns;
    List<Expression> refererColumnReferences
      = new ArrayList(refererColumns.size());

    for (Column c: refererColumns) {
      refererColumnReferences.add(reference(referer, c));
    }

    refererQuery = new QueryTemplate
      (refererColumnReferences, referer, refererTest);
    
    TableReference referent = reference(constraint.referentTable);

    Expression referentTest = constant(true);

    for (Column c: constraint.referentColumns) {
      referentTest = and
        (equal(reference(referent, c), parameter()), referentTest);
    }
    
    List<Column> referentColumns = referent.table.primaryKey.columns;
    List<Expression> referentColumnReferences
      = new ArrayList(refererColumns.size());

    for (Column c: referentColumns) {
      referentColumnReferences.add(reference(referent, c));
    }

    referentQuery = new QueryTemplate
      (referentColumnReferences, referent, referentTest);
  }

  private Object[] parameters(List<Column> columns, Node tree) {
    Object[] parameters = new Object[columns.size()];
    for (int i = 0; i < parameters.length; ++i) {
      Node n = Node.find(tree, columns.get(i));
      parameters[i] = n == Node.Null ? null : n.value;
    }
    return parameters;
  }

  private QueryResult query(QueryTemplate query,
                            Revision revision, List<Column> columns, Node tree)
  {
    return MyRevision.Empty.diff(revision, query, parameters(columns, tree));
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
