package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.DuplicateKeyException;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

class InsertTemplateAdapter implements PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters)
  {
    InsertTemplate insert = (InsertTemplate) template;

    ExpressionContext expressionContext = new ExpressionContext(parameters);

    Map<Column, Object> map = new HashMap();
    { int index = 0;
      Iterator<Column> columnIterator = insert.columns.iterator();
      Iterator<Expression> valueIterator = insert.values.iterator();
      while (columnIterator.hasNext()) {
        Column column = columnIterator.next();
        Object value = ExpressionAdapterFactory.makeAdapter
          (expressionContext, valueIterator.next()).evaluate(false);

        if (value == null || column.type.isInstance(value)) {
          map.put(column, value);
        } else {
          throw new ClassCastException
            (value.getClass() + " cannot be cast to " + column.type
             + " in column " + index);
        }

        ++ index;
      }
    }
      
    Node tree = Node.Null;
    Node.BlazeResult result = new Node.BlazeResult();
    for (Column c: insert.columns) {
      tree = Node.blaze(result, builder.token, builder.stack, tree, c);
      result.node.value = map.get(c);
    }

    builder.prepareForUpdate(insert.table);

    Index index = insert.table.primaryKey;

    builder.setKey(Constants.TableDataDepth, insert.table);
    builder.setKey(Constants.IndexDataDepth, index);

    List<Column> columns = index.columns;
    int i;
    for (i = 0; i < columns.size() - 1; ++i) {
      builder.setKey
        (i + Constants.IndexDataBodyDepth,
         (Comparable) map.get(columns.get(i)));
    }

    Node n = builder.blaze
      (i + Constants.IndexDataBodyDepth, (Comparable) map.get(columns.get(i)));

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
