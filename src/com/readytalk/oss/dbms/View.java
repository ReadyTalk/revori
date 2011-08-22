package com.readytalk.oss.dbms;

import static com.readytalk.oss.dbms.util.Util.compare;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public final class View implements Comparable<View> {
  public final QueryTemplate query;
  public final List<Column> columns;
  public final Table table;
  public final int primaryKeyOffset;
  public final int aggregateOffset;
  public final int aggregateExpressionOffset;

  public View(QueryTemplate query,
              List<Column> columns,
              List<Column> primaryKey,
              final List<Expression> primaryKeyExpressions,
              String id)
  {
    final List<Column> myPrimaryKey = new ArrayList();
    final List<Column> myColumns = new ArrayList(columns);
    final List<Expression> myExpressions = new ArrayList(query.expressions);

    myExpressions.addAll(primaryKeyExpressions);

    if (query.groupingExpressions.isEmpty()) {
      query.source.visit(new SourceVisitor() {
          public void visit(Source source) {
            if (source instanceof TableReference) {
              TableReference tableReference = (TableReference) source;
              for (Column c: tableReference.table.primaryKey.columns) {
                ColumnReference columnReference = new ColumnReference
                  (tableReference, c);

                if (! primaryKeyExpressions.contains(columnReference)) {
                  myPrimaryKey.add(new Column(c.type));
                  myExpressions.add(columnReference);
                }
              }
            }
          }
        });
    } else {
      for (Expression e: query.groupingExpressions) {
        if (! primaryKeyExpressions.contains(e)) {
          Class type = e.typeConstraint();
          if (type == null) throw new NullPointerException();

          myPrimaryKey.add(new Column(type));
          myExpressions.add(e);
        }
      }
    }

    myColumns.addAll(myPrimaryKey);

    this.table = new Table(myPrimaryKey, id);
    this.primaryKeyOffset = query.expressions.size();
    this.aggregateOffset = myExpressions.size();

    if (! query.groupingExpressions.isEmpty()) {
      myColumns.add(new Column(Integer.class));
      myExpressions.add
        (new Aggregate
         (Integer.class, Foldables.Count, Collections.emptyList()));

      { ExpressionVisitor visitor = new ExpressionVisitor() {
          public void visit(Expression e) {
            if (e instanceof Aggregate) {
              myColumns.add(new Column(((Aggregate) e).type));
              myExpressions.add(e);
            }
          }
        };

        for (Expression e: query.expressions) {
          e.visit(visitor);
        }
      }
    }

    this.aggregateExpressionOffset = myExpressions.size();

    if (! query.groupingExpressions.isEmpty()) {
      { ExpressionVisitor visitor = new ExpressionVisitor() {
          public void visit(Expression e) {
            if (e instanceof Aggregate) {
              for (Expression ae:
                     ((List<Expression>) ((Aggregate) e).expressions))
              {
                myExpressions.add(ae);
              }
            }
          }
        };

        for (Expression e: query.expressions) {
          e.visit(visitor);
        }
      }
    }

     this.query = new QueryTemplate
      (myExpressions, query.source, query.test, query.groupingExpressions);
    this.columns = Collections.unmodifiableList(myColumns);
  }

  public int compareTo(View o) {
    if (o == this) return 0;

    int d = table.compareTo(o.table);
    if (d != 0) {
      return d;
    }

    d = compare(columns, o.columns);
    if (d != 0) {
      return d;
    }

    return query.compareTo(o.query);
  }

  public int hashCode() {
    return table.id.hashCode();
  }

  /**
   * Returns true if and only if the specified object is a Table and
   * its ID and primaryKey are equal to those of this instance.
   */
  public boolean equals(Object o) {
    return o instanceof View && compareTo((View) o) == 0;
  }

  public String toString() {
    return "view[" + table + "]";
  }
}
