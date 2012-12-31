/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.Table;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Index;
import com.readytalk.revori.Column;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.DuplicateKeyException;
import com.readytalk.revori.Comparators;

import java.util.List;
import java.util.ArrayList;

class UpdateTemplateAdapter implements PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters)
  {
    UpdateTemplate update = (UpdateTemplate) template;

    ExpressionContext expressionContext = new ExpressionContext(parameters, null);

    ExpressionAdapter test = ExpressionAdapterFactory.makeAdapter
      (expressionContext, update.test);

    List<ExpressionAdapter> valueAdapters = new ArrayList<ExpressionAdapter>
      (update.values.size());

    for (Expression e: update.values) {
      valueAdapters.add
        (ExpressionAdapterFactory.makeAdapter(expressionContext, e));
    }

    Table table = update.tableReference.table;

    builder.setKey(Constants.TableDataDepth, table, Compare.TableComparator);

    Plan plan = Plan.choosePlan
      (MyRevision.Empty, NodeStack.Null, builder.result, builder.stack, test,
       update.tableReference);

    builder.updateIndex(plan.index);

    Object[] values = new Object[update.columns.size()];
    Node.BlazeResult result = new Node.BlazeResult();
    int count = 0;
    MyRevision revision = builder.result;

    Index index = table.primaryKey;

    builder.setKey(Constants.IndexDataDepth, index, Compare.IndexComparator);

    TableIterator iterator = new TableIterator
      (update.tableReference, MyRevision.Empty, NodeStack.Null, revision,
       new NodeStack(), test, expressionContext, plan, false);

    List<Column<?>> keyColumns = index.columns;

    int[] keyColumnsUpdated;
    { List<Column<?>> columnList = new ArrayList<Column<?>>();
      for (Column<?> c: keyColumns) {
        if (update.columns.contains(c)) {
          if (columnList == null) {
            columnList = new ArrayList<Column<?>>();
          }
          columnList.add(c);
        }
      }

      if (columnList.isEmpty()) {
        keyColumnsUpdated = null;
      } else {
        keyColumnsUpdated = new int[columnList.size()];
        for (int i = 0; i < keyColumnsUpdated.length; ++i) {
          keyColumnsUpdated[i] = update.columns.indexOf(columnList.get(i));
        }
      }
    }
        
    Object updateToken = index.equals(plan.index) ? null : builder.token;

    count = 0;
    boolean done = false;
    while (! done) {
      QueryResult.Type type = iterator.nextRow();
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        builder.prepareForUpdate(table);

        ++ count;

        for (int i = 0; i < update.columns.size(); ++i) {
          values[i] = valueAdapters.get(i).evaluate(false);
        }

        Node original = (Node) iterator.pair.fork.value;

        boolean keyValuesChanged = false;
        if (keyColumnsUpdated != null) {
          // some of the columns in the current index are being
          // updated, but we don't need to remove and reinsert the
          // row unless at least one is actually changing to a new
          // value
          for (int columnIndex: keyColumnsUpdated) {
            Column c = keyColumns.get(columnIndex);
            if (! Compare.equal
                (values[columnIndex], Node.find
                 (original, c, Compare.ColumnComparator).value, c.comparator))
            {
              keyValuesChanged = true;
              break;
            }
          }

          if (! keyValuesChanged) {
            break;
          }

          if (updateToken == null) {
            builder.setToken(updateToken = new Object());
          }

          int i = 0;
          for (; i < keyColumns.size() - 1; ++i) {
            Column c = keyColumns.get(i);
            builder.setKey
              (i + Constants.IndexDataBodyDepth,
               Node.find(original, c, Compare.ColumnComparator).value,
               c.comparator);
          }

          Column c = keyColumns.get(i);
          builder.deleteKey
            (i + Constants.IndexDataBodyDepth,
             Node.find(original, c, Compare.ColumnComparator).value,
             c.comparator);
        }

        Node tree = original;

        for (int i = 0; i < update.columns.size(); ++i) {
          Column<?> column = update.columns.get(i);
          Object value = Compare.validate(values[i], column.type);

          if (value == null) {
            tree = Node.delete
              (builder.token, builder.stack, tree, column,
               Compare.ColumnComparator);
          } else {
            tree = Node.blaze
              (result, builder.token, builder.stack, tree, column,
               Compare.ColumnComparator);
            result.node.value = value;
          }
        }

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
            Column c = keyColumns.get(i);
          builder.setKey
            (i + Constants.IndexDataBodyDepth,
             Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
        }

        Column c = keyColumns.get(i);
        Node n = builder.blaze
          (i + Constants.IndexDataBodyDepth,
           Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);

        if (n.value == Node.Null || (! keyValuesChanged)) {
          n.value = tree;
        } else {
          throw new DuplicateKeyException();
        }
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
      
    return count;
  }
}
