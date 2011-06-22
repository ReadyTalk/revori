package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.DuplicateKeyException;

import java.util.List;
import java.util.ArrayList;

class UpdateTemplateAdapter implements PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters)
  {
    UpdateTemplate update = (UpdateTemplate) template;

    ExpressionContext expressionContext = new ExpressionContext(parameters);

    ExpressionAdapter test = ExpressionAdapterFactory.makeAdapter
      (expressionContext, update.test);

    List<ExpressionAdapter> valueAdapters = new ArrayList
      (update.values.size());

    for (Expression e: update.values) {
      valueAdapters.add
        (ExpressionAdapterFactory.makeAdapter(expressionContext, e));
    }

    Table table = update.tableReference.table;

    builder.setKey(Constants.TableDataDepth, table);

    Plan plan = Plan.choosePlan
      (MyRevision.Empty, NodeStack.Null, builder.result, builder.stack, test,
       update.tableReference);

    builder.updateIndex(plan.index);

    Object[] values = new Object[update.columns.size()];
    Node.BlazeResult result = new Node.BlazeResult();
    int count = 0;
    MyRevision revision = builder.result;

    Index index = table.primaryKey;

    builder.setKey(Constants.IndexDataDepth, index);

    TableIterator iterator = new TableIterator
      (update.tableReference, MyRevision.Empty, NodeStack.Null, revision,
       new NodeStack(), test, expressionContext, plan, false);

    List<RefererForeignKeyAdapter> refererKeyAdapters
      = builder.getRefererForeignKeyAdapters(table);

    List<ReferentForeignKeyAdapter> referentKeyAdapters
      = builder.getReferentForeignKeyAdapters(table);

    List<Column> keyColumns = index.columns;

    int[] keyColumnsUpdated;
    { List<Column> columnList = new ArrayList();
      for (Column c: keyColumns) {
        if (update.columns.contains(c)) {
          if (columnList == null) {
            columnList = new ArrayList();
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
            if (! Compare.equal(values[columnIndex], Node.find
                                (original, keyColumns.get(columnIndex)).value))
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
            builder.setKey
              (i + Constants.IndexDataBodyDepth,
               (Comparable) Node.find(original, keyColumns.get(i)).value);
          }

          builder.delete
            (i + Constants.IndexDataBodyDepth,
             (Comparable) Node.find(original, keyColumns.get(i)).value);
        }

        if (updateToken == null && (! (referentKeyAdapters.isEmpty()))) {
          // ensure the original tree remains unchanged so we can use
          // it to validate foreign key constraints:
          builder.setToken(updateToken = new Object());
        }

        Node tree = original;

        for (int i = 0; i < update.columns.size(); ++i) {
          Column column = update.columns.get(i);
          Object value = Compare.coerce(values[i], column.type);

          if (value == null) {
            tree = Node.delete(builder.token, builder.stack, tree, column);
          } else {
            tree = Node.blaze
              (result, builder.token, builder.stack, tree, column);
            result.node.value = value;
          }
        }

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          builder.setKey
            (i + Constants.IndexDataBodyDepth,
             (Comparable) Node.find(tree, keyColumns.get(i)).value);
        }

        Node n = builder.blaze
          (i + Constants.IndexDataBodyDepth,
           (Comparable) Node.find(tree, keyColumns.get(i)).value);

        if (n.value == Node.Null || (! keyValuesChanged)) {
          n.value = tree;
        } else {
          throw new DuplicateKeyException();
        }

        for (RefererForeignKeyAdapter adapter: refererKeyAdapters) {
          adapter.handleInsert(builder, tree);
        }

        for (ReferentForeignKeyAdapter adapter: referentKeyAdapters) {
          adapter.handleUpdate(builder, original);
        }
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
      
    return count;
  }
}
