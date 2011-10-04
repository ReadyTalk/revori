package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.Comparators;

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
