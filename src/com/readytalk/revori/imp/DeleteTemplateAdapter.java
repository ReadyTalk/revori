/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.Table;
import com.readytalk.revori.Index;
import com.readytalk.revori.Column;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.Comparators;

import java.util.List;

class DeleteTemplateAdapter implements PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters)
  {
    DeleteTemplate delete = (DeleteTemplate) template;

    ExpressionContext expressionContext = new ExpressionContext(parameters, null);

    ExpressionAdapter test = ExpressionAdapterFactory.makeAdapter
      (expressionContext, delete.test);

    builder.setKey
      (Constants.TableDataDepth, delete.tableReference.table,
       Compare.TableComparator);

    Plan plan = Plan.choosePlan
      (MyRevision.Empty, NodeStack.Null, builder.result, builder.stack, test,
       delete.tableReference);

    builder.updateIndex(plan.index);

    int count = 0;
    MyRevision revision = builder.result;
    Table table = delete.tableReference.table;
    Index index = table.primaryKey;

    builder.setKey(Constants.IndexDataDepth, index, Compare.IndexComparator);

    TableIterator iterator = new TableIterator
      (delete.tableReference, MyRevision.Empty, NodeStack.Null, revision,
       new NodeStack(), test, expressionContext, plan, false);

    List<Column<?>> keyColumns = index.columns;

    Object deleteToken = index.equals(plan.index) ? null : builder.token;

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

        if (deleteToken == null) {
          builder.setToken(deleteToken = new Object());
        }

        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          Column c = keyColumns.get(i);
          builder.setKey
            (i + Constants.IndexDataBodyDepth,
             Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
        }

        Column c = keyColumns.get(i);
        builder.deleteKey
          (i + Constants.IndexDataBodyDepth,
           Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }

    return count;
  }
}
