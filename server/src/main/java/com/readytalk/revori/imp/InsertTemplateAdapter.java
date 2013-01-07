/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.readytalk.revori.Column;
import com.readytalk.revori.DuplicateKeyException;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Index;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.PatchTemplate;

class InsertTemplateAdapter implements PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters)
  {
    InsertTemplate insert = (InsertTemplate) template;

    ExpressionContext expressionContext = new ExpressionContext(parameters, null);

    Map<Column<?>, Object> map = new HashMap<Column<?>, Object>();
    { int index = 0;
      Iterator<Column<?>> columnIterator = insert.columns.iterator();
      Iterator<Expression> valueIterator = insert.values.iterator();
      while (columnIterator.hasNext()) {
        Column<?> column = columnIterator.next();

        map.put
          (column, Compare.validate
           (ExpressionAdapterFactory.makeAdapter
            (expressionContext, valueIterator.next()).evaluate(false),
            column.type));

        ++ index;
      }
    }
      
    Node tree = Node.Null;
    Node.BlazeResult result = new Node.BlazeResult();
    for (Column<?> c: insert.columns) {
      tree = Node.blaze
        (result, builder.token, builder.stack, tree, c,
         Compare.ColumnComparator);
      result.node.value = map.get(c);
    }

    builder.prepareForUpdate(insert.table);

    Index index = insert.table.primaryKey;

    builder.setKey(Constants.TableDataDepth, insert.table,
                   Compare.TableComparator);
    builder.setKey(Constants.IndexDataDepth, index, Compare.IndexComparator);

    List<Column<?>> columns = index.columns;
    int i;
    for (i = 0; i < columns.size() - 1; ++i) {
      Column c = columns.get(i);
      builder.setKey
        (i + Constants.IndexDataBodyDepth, map.get(c), c.comparator);
    }

    Column c = columns.get(i);
    Node n = builder.blaze
      (i + Constants.IndexDataBodyDepth, map.get(c), c.comparator);

    if (n.value == Node.Null) {
      n.value = tree;
      return 1;
    } else {
      switch (insert.duplicateKeyResolution) {
      case Skip:
        return 0;

      case Overwrite:
        n.value = tree;
        return 1;

      case Throw:
        throw new DuplicateKeyException();

      default:
        throw new RuntimeException
          ("unexpected resolution: " + insert.duplicateKeyResolution);
      }
    }
  }
}
