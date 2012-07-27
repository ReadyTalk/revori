/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.compare;

import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

public final class View implements Comparable<View> {
  public final QueryTemplate query;
  public final List<Object> parameters;
  public final List<Column<?>> columns;
  public final Table table;
  public final int primaryKeyOffset;
  public final int aggregateOffset;
  public final int aggregateExpressionOffset;

  public View(QueryTemplate query,
              List<Object> parameters,
              List<Column<?>> columns,
              List<Column<?>> primaryKey,
              final List<Expression> primaryKeyExpressions,
              String id)
  {
    final List<Column<?>> myPrimaryKey = new ArrayList<Column<?>>(primaryKey);
    final List<Column<?>> myColumns = new ArrayList<Column<?>>(columns);
    final List<Expression> myExpressions = new ArrayList<Expression>(query.expressions);

    if (myColumns.size() != myExpressions.size()) {
      throw new IllegalArgumentException();
    }

    myExpressions.addAll(primaryKeyExpressions);

    if (myColumns.size() + myPrimaryKey.size() != myExpressions.size()) {
      throw new IllegalArgumentException();
    }

    for (QueryTemplate.OrderExpression e: query.orderByExpressions) {
      myPrimaryKey.add
        (makeColumn(e.expression, Comparable.class, e.comparator));
      myExpressions.add(e.expression);
    }

    if (query.hasAggregates) {
      for (Expression e: query.groupingExpressions) {
        if (! primaryKeyExpressions.contains(e)) {
          myPrimaryKey.add
            (makeColumn(e, Comparable.class, Comparators.Ascending));
          myExpressions.add(e);
        }
      }
    } else {
      query.source.visit(new SourceVisitor() {
          public void visit(Source source) {
            if (source instanceof TableReference) {
              TableReference tableReference = (TableReference) source;
              for (Column<?> c: tableReference.table.primaryKey.columns) {
                ColumnReference columnReference = new ColumnReference
                  (tableReference, c);

                if (! primaryKeyExpressions.contains(columnReference)) {
                  myPrimaryKey.add
                    (makeColumn(columnReference, null, Comparators.Ascending));
                  myExpressions.add(columnReference);
                }
              }
            }
          }
        });
    }

    if (myPrimaryKey.isEmpty()) {
      myPrimaryKey.add
        (new Column<Singleton>(Singleton.class, "singleton." + Column.makeId()));
      myExpressions.add(new Constant(Singleton.Instance));
    }

    myColumns.addAll(myPrimaryKey);

    this.table = new Table(myPrimaryKey, id);
    this.primaryKeyOffset = query.expressions.size();
    this.aggregateOffset = myExpressions.size();

    final Set<Aggregate<?>> aggregates;
    if (query.hasAggregates) {
      myColumns.add(new Column<Integer>(Integer.class, "count." + Column.makeId()));
      myExpressions.add
        (new Aggregate<Integer>
         (Integer.class, Foldables.Count, new ArrayList<Expression>()));

      aggregates = new TreeSet<Aggregate<?>>();

      ExpressionVisitor visitor = new ExpressionVisitor() {
          public void visit(Expression e) {
            if (e instanceof Aggregate) {
              aggregates.add((Aggregate<?>) e);
            }
          }
        };

      for (Expression e: query.expressions) {
        e.visit(visitor);
      }

      for (Aggregate<?> a: aggregates) {
        myColumns.add(makeColumn(a, null, Comparators.Ascending));
        myExpressions.add(a);
      }
    } else {
      aggregates = null;
    }

    this.aggregateExpressionOffset = myExpressions.size();

    if (query.hasAggregates) {
      for (Aggregate a: aggregates) {
        for (Expression e: (List<Expression>) (List) a.expressions) {
          myExpressions.add(e);
        }
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
    this(query, list(parameters),
         makeColumnList(query.expressions),
         Collections.<Column<?>>emptyList(),
         Collections.<Expression>emptyList(),
         makeId(query.source));
  }

  private static String makeId(Source source) {
    String id = Table.makeId();
    if (source instanceof TableReference) {
      id = ((TableReference) source).table.id + "." + id;
    }
    return "view." + id;
  }

  private static <T> Column<T> makeColumn(Expression e, Class<T> defaultType,
                                          Comparator comparator)
  {
    Class<T> type;
    String id = Column.makeId();
    if (e instanceof ColumnReference) {
      Column<T> c = ((ColumnReference<T>) e).column;
      type = c.type;
      id = c.id + "." + id;
    } else if (e instanceof Aggregate) {
      type = ((Aggregate<T>) e).type;
      id = "aggregate." + id;
    } else {
      type = defaultType;
    }
    return new Column<T>(type, id, comparator);
  }

  private static List<Column<?>> makeColumnList(List<Expression> expressions) {
    List<Column<?>> columns = new ArrayList<Column<?>>(expressions.size());
    for (int i = 0; i < expressions.size(); ++i) {
      columns.add
        (makeColumn(expressions.get(i), Object.class, Comparators.Ascending));
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
