package com.readytalk.oss.dbms;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.compare;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public final class View implements Comparable<View> {
  public final QueryTemplate query;
  public final List<Object> parameters;
  public final List<Column> columns;
  public final Table table;
  public final int primaryKeyOffset;
  public final int aggregateOffset;
  public final int aggregateExpressionOffset;

  public View(QueryTemplate query,
              List<Object> parameters,
              List<Column> columns,
              List<Column> primaryKey,
              final List<Expression> primaryKeyExpressions,
              String id)
  {
    final List<Column> myPrimaryKey = new ArrayList(primaryKey);
    final List<Column> myColumns = new ArrayList(columns);
    final List<Expression> myExpressions = new ArrayList(query.expressions);

    myExpressions.addAll(primaryKeyExpressions);

    if (query.hasAggregates) {
      for (Expression e: query.groupingExpressions) {
        if (! primaryKeyExpressions.contains(e)) {
          myPrimaryKey.add(makeColumn(e, Comparable.class));
          myExpressions.add(e);
        }
      }
    } else {
      query.source.visit(new SourceVisitor() {
          public void visit(Source source) {
            if (source instanceof TableReference) {
              TableReference tableReference = (TableReference) source;
              for (Column c: tableReference.table.primaryKey.columns) {
                ColumnReference columnReference = new ColumnReference
                  (tableReference, c);

                if (! primaryKeyExpressions.contains(columnReference)) {
                  myPrimaryKey.add(makeColumn(columnReference, null));
                  myExpressions.add(columnReference);
                }
              }
            }
          }
        });
    }

    if (myPrimaryKey.isEmpty()) {
      myPrimaryKey.add
        (new Column(Singleton.class, "singleton." + Column.makeId()));
      myExpressions.add(new Constant(Singleton.Instance));
    }

    myColumns.addAll(myPrimaryKey);

    this.table = new Table(myPrimaryKey, id);
    this.primaryKeyOffset = query.expressions.size();
    this.aggregateOffset = myExpressions.size();

    if (query.hasAggregates) {
      myColumns.add(new Column(Integer.class, "count." + Column.makeId()));
      myExpressions.add
        (new Aggregate
         (Integer.class, Foldables.Count, Collections.emptyList()));

      ExpressionVisitor visitor = new ExpressionVisitor() {
          public void visit(Expression e) {
            if (e instanceof Aggregate) {
              myColumns.add(makeColumn(e, null));

              myExpressions.add(e);
            }
          }
        };

      for (Expression e: query.expressions) {
        e.visit(visitor);
      }
    }

    this.aggregateExpressionOffset = myExpressions.size();

    if (query.hasAggregates) {
      ExpressionVisitor visitor = new ExpressionVisitor() {
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

    this.query = new QueryTemplate
      (myExpressions, query.source, query.test, query.groupingExpressions);
    this.parameters = Collections.unmodifiableList(parameters);
    this.columns = Collections.unmodifiableList(myColumns);
  }

  public View(QueryTemplate query,
              Object[] parameters)
  {
    this(query, list(parameters), makeColumnList(query.expressions),
         (List<Column>) (List) Collections.emptyList(),
         (List<Expression>) (List) Collections.emptyList(),
         makeId(query.source));
  }

  private static String makeId(Source source) {
    String id = Table.makeId();
    if (source instanceof TableReference) {
      id = ((TableReference) source).table.id + "." + id;
    }
    return "view." + id;
  }

  private static Column makeColumn(Expression e, Class defaultType) {
    Class type;
    String id = Column.makeId();
    if (e instanceof ColumnReference) {
      Column c = ((ColumnReference) e).column;
      type = c.type;
      id = c.id + "." + id;
    } else if (e instanceof Aggregate) {
      type = ((Aggregate) e).type;
      id = "aggregate." + id;
    } else {
      type = defaultType;
    }
    return new Column(type, id);
  }

  private static List<Column> makeColumnList(List<Expression> expressions) {
    List<Column> columns = new ArrayList(expressions.size());
    for (int i = 0; i < expressions.size(); ++i) {
      columns.add(makeColumn(expressions.get(i), Object.class));
    }
    return columns;
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
