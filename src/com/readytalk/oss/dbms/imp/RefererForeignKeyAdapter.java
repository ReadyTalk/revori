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
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyException;

import java.util.List;
import java.util.Collections;

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
      Node n = Node.find(tree, columns.get(i));
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
    if(params == null) {
      return false;
    }
    return MyRevision.Empty.diff
      (revision, query, params).nextRow() == QueryResult.Type.End;
  }

  public void handleInsert(MyRevisionBuilder builder, Node tree) {
    if (queryEmptyAndNotNull(query, builder.result, constraint.refererColumns, tree)) {
      throw new ForeignKeyException();
    }
  }

  public boolean isBrokenReference(Revision revision, Node tree) {
    return queryEmptyAndNotNull(query, revision, constraint.refererColumns, tree);
  }
}
