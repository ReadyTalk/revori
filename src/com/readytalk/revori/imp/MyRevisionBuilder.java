/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.expect;
import static com.readytalk.revori.util.Util.list;
import static com.readytalk.revori.util.Util.copy;

import static com.readytalk.revori.SourceFactory.reference;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.TableBuilder;
import com.readytalk.revori.RowBuilder;
import com.readytalk.revori.Index;
import com.readytalk.revori.View;
import com.readytalk.revori.Table;
import com.readytalk.revori.Column;
import com.readytalk.revori.Revision;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.DuplicateKeyException;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.ColumnList;
import com.readytalk.revori.ForeignKey;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.SourceVisitor;
import com.readytalk.revori.Source;
import com.readytalk.revori.Comparators;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

class MyRevisionBuilder implements RevisionBuilder {
  private static final Map<Class, PatchTemplateAdapter> adapters
    = new HashMap<Class, PatchTemplateAdapter>();

  static {
    adapters.put(UpdateTemplate.class, new UpdateTemplateAdapter());
    adapters.put(InsertTemplate.class, new InsertTemplateAdapter());
    adapters.put(DeleteTemplate.class, new DeleteTemplateAdapter());
  }

  public Object token;
  public final NodeStack stack;
  public final Object[] keys;
  public final Comparator[] comparators;
  public final Node[] blazedRoots;
  public final Node[] blazedLeaves;
  public final Node[] found;
  public final Node.BlazeResult blazeResult = new Node.BlazeResult();
  public NodeStack indexUpdateIterateStack;
  public NodeStack indexUpdateBaseStack;
  public NodeStack indexUpdateForkStack;
  public MyRevision base;
  public MyRevision indexBase;
  public MyRevision result;
  public int max = -1;
  public boolean dirtyIndexes;

  public MyRevisionBuilder(Object token,
                           MyRevision base,
                           NodeStack stack)
  {
    this.token = token;
    this.base = base;
    this.indexBase = base;
    this.result = base;
    this.stack = stack;
    keys = new Object[Constants.MaxDepth + 1];
    comparators = new Comparator[Constants.MaxDepth + 1];
    blazedRoots = new Node[Constants.MaxDepth + 1];
    blazedLeaves = new Node[Constants.MaxDepth + 1];
    found = new Node[Constants.MaxDepth + 1];
  }

  public void setToken(Object token) {
    if (token != this.token) {
      this.token = token;
      for (int i = 0; i < max; ++i) {
        found[i] = null;
        blazedLeaves[i] = null;
        blazedRoots[i + 1] = null;
      }
    }
  }

  public void setKey(int index, Object key, Comparator comparator) {
    if (key == Compare.Undefined) throw new RuntimeException();
    if (max < index || (! Compare.equal(key, keys[index], comparator))) {
      max = index;
      keys[index] = key;
      comparators[index] = comparator;
      found[index] = null;
      blazedLeaves[index] = null;
      blazedRoots[index + 1] = null;
    }
  }

  public Node blaze(int index, Object key, Comparator comparator) {
    setKey(index, key, comparator);
    return blaze(index);
  }

  public void insertOrUpdate(int index, Object key, Comparator comparator,
                             Object value)
  {
    blaze(index, key, comparator).value = value;
  }

  public void deleteKey(int index, Object key, Comparator comparator) {
    setKey(index, key, comparator);
    delete(index);
  }

  public void deleteAll() {
    result = MyRevision.Empty;
    max = -1;
  }

  private void delete(int index) {
    Node root = blazedRoots[index];
    if (root == null) {
      if (index == 0) {
        root = Node.delete(token, stack, result.root, keys[0], comparators[0]);

        if (root != result.root) {
          result = getRevision(token, result, root);
        }
      } else {
        Node original = find(index);
        Node originalRoot = (Node) find(index - 1).value;

        if (original == Node.Null) {
          return;
        } else if (original == originalRoot
                   && original.left == Node.Null
                   && original.right == Node.Null)
        {
          delete(index - 1);
        } else {
          root = Node.delete
            (token, stack, (Node) blaze(index - 1).value, keys[index],
             comparators[index]);
          blazedLeaves[index - 1].value = root;
          blazedRoots[index] = root;
          blazedLeaves[index] = null;
        }
      }
    } else {
      deleteBlazed(index);
    }

    if (max >= index) {
      max = index - 1;
    }
  }

  private Node find(int index) {
    Node n = blazedLeaves[index];
    if (n == null) {
      n = found[index];
      if (n == null) {
        if (index == 0) {
          n = Node.find(result.root, keys[0], comparators[0]);
          found[0] = n;
        } else {
          n = Node.find
            ((Node) find(index - 1).value, keys[index], comparators[index]);
          found[index] = n;
        }
      }
    }
    return n;
  }

  private void deleteBlazed(int index) {
    blazedLeaves[index] = null;
    Node root = Node.delete
      (token, stack, blazedRoots[index], keys[index], comparators[index]);
    blazedRoots[index] = root;
    blazedLeaves[index] = null;
    if (root == Node.Null) {
      if (index == 0) {
        root = Node.delete
          (token, stack, result.root, keys[0], comparators[0]);

        if (root != result.root) {
          result = getRevision(token, result, root);
        }
      } else {
        delete(index - 1);
      }
    } else {
      if (index == 0) {
        if (root != result.root) {
          result = getRevision(token, result, root);
        }
      } else {
        blazedLeaves[index - 1].value = root;
      }
    }
  }

  private Node blaze(int index) {
    Node n = blazedLeaves[index];
    if (n == null) {
      if (index == 0) {
        Node root = Node.blaze
          (blazeResult, token, stack, result.root, keys[0], comparators[0]);

        if (root != result.root) {
          result = getRevision(token, result, root);
        }

        blazedRoots[0] = root;
        blazedLeaves[0] = blazeResult.node;
        blazedRoots[1] = (Node) blazeResult.node.value;
        return blazeResult.node;
      } else {
        Node root = Node.blaze
          (blazeResult, token, stack, (Node) blaze(index - 1).value,
           keys[index], comparators[index]);

        blazedLeaves[index - 1].value = root;
        blazedRoots[index] = root;
        blazedLeaves[index] = blazeResult.node;
        return blazeResult.node;
      }
    } else {
      return n;
    }
  }

  public void updateIndexTree(Index index,
                              MyRevision base,
                              NodeStack baseStack,
                              NodeStack forkStack)
  {
    expect(! index.equals(index.table.primaryKey));

    TableIterator iterator
      = new TableIterator
      (reference(index.table), base, baseStack, result, forkStack,
       ConstantAdapter.True, new ExpressionContext(null, null), false);

    setKey(Constants.TableDataDepth, index.table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, index, Compare.IndexComparator);

    List<Column<?>> keyColumns = index.columns;

    while (true) {
      QueryResult.Type type = iterator.nextRow();
      switch (type) {
      case End:
        return;
      
      case Inserted: {
        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          Column c = keyColumns.get(i);
          setKey
            (i + Constants.IndexDataBodyDepth,
             Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
        }

        Column c = keyColumns.get(i);
        Node n = blaze
          (i + Constants.IndexDataBodyDepth,
           Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);

        expect(n.value == Node.Null);
      
        n.value = tree;
      } break;

      case Deleted: {
        Node tree = (Node) iterator.pair.base.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          Column c = keyColumns.get(i);
          setKey
            (i + Constants.IndexDataBodyDepth,
             Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
        }

        Column c = keyColumns.get(i);
        deleteKey
          (i + Constants.IndexDataBodyDepth,
           Node.find(tree, c, Compare.ColumnComparator).value, c.comparator);
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  private Node makeTree(NodeStack stack,
                        List<Column<?>> columns,
                        List<ExpressionAdapter> expressions)
  {
    Node.BlazeResult result = new Node.BlazeResult();
    Node n = Node.Null;
    for (int i = 0; i < columns.size(); ++i) {
      Column<?> c = columns.get(i);
      Object v = expressions.get(i).evaluate(true);
      if (! Compare.Undefined.equals(v)) {
        if (v != null && ! c.type.isInstance(v)) {
          throw new ClassCastException
            (v.getClass().getName() + " cannot be cast to "
             + c.type.getName());
        }
        n = Node.blaze(result, token, stack, n, c, Compare.ColumnComparator);
        result.node.value = v;
      }
    }
    return n;
  }

  public void updateViewTree(View view,
                             MyRevision base,
                             NodeStack baseStack,
                             NodeStack forkStack)
  {
    MyQueryResult qr = new MyQueryResult
      (base, baseStack, result, forkStack, view.query,
       view.parameters.toArray(new Object[view.parameters.size()]), true);

    setKey(Constants.TableDataDepth, view.table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, view.table.primaryKey,
           Compare.IndexComparator);

    List<Column<?>> keyColumns = view.table.primaryKey.columns;

    final List<ExpressionAdapter> expressions = qr.expressions;

    List<AggregateAdapter> aggregates;
    int maxValues;
    if (view.query.hasAggregates) {
      aggregates = new ArrayList<AggregateAdapter>();

      maxValues = 0;
      for (int i = view.aggregateOffset; i < view.aggregateExpressionOffset;
           ++i)
      {
        AggregateAdapter aa = (AggregateAdapter) expressions.get(i);
        if (maxValues < aa.aggregate.expressions.size()) {
          maxValues = aa.aggregate.expressions.size();
        }
        aggregates.add(aa);
      }
    } else {
      maxValues = 0;
      aggregates = null;
    }

    Object[] values = new Object[maxValues];
    NodeStack stack = new NodeStack();

    boolean sawSomething = false;
    boolean done = false;
    while (! done) {
      QueryResult.Type type = qr.nextRow();
      if (! QueryResult.Type.End.equals(type)) {
        sawSomething = true;
      }
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             expressions.get(view.primaryKeyOffset + i).evaluate(true),
             keyColumns.get(i).comparator);
        }

        Node n = blaze
          (i + Constants.IndexDataBodyDepth,
           expressions.get(view.primaryKeyOffset + i).evaluate(true),
           keyColumns.get(i).comparator);

        // System.out.println("inserted " + com.readytalk.revori.util.Util.toString(keys, 0, Constants.IndexDataBodyDepth + i + 1));

        if (view.query.hasAggregates) {
          int columnOffset = view.aggregateOffset;
          int expressionOffset = view.aggregateExpressionOffset;
          for (AggregateAdapter a: aggregates) {
            for (int j = 0; j < a.aggregate.expressions.size(); ++j) {
              values[j] = expressions.get(expressionOffset++).evaluate(true);
            }

            Node old = Node.find
               ((Node) n.value, view.columns.get(columnOffset++),
                Compare.ColumnComparator);

            a.add(old == Node.Null ? a.aggregate.function.base() : old.value,
                  values);
          }
        } else {
          expect(n.value == Node.Null);
        }
      
        n.value = makeTree(stack, view.columns, expressions);

        if (view.query.hasAggregates) {
          for (AggregateAdapter a: aggregates) {
            a.value = Compare.Undefined;
          }
        }
      } break;

      case Deleted: {
        for (int i = 0; i < keyColumns.size(); ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             expressions.get(view.primaryKeyOffset + i).evaluate(true),
             keyColumns.get(i).comparator);
        }
        int index = keyColumns.size() - 1 + Constants.IndexDataBodyDepth;

        // System.out.println("deleted " + com.readytalk.revori.util.Util.toString(keys, 0, index + 1));

        if (view.query.hasAggregates) {
          Node n = find(index);

          int columnOffset = view.aggregateOffset;
          int expressionOffset = view.aggregateExpressionOffset;
          for (AggregateAdapter a: aggregates) {
            for (int j = 0; j < a.aggregate.expressions.size(); ++j) {
              values[j] = expressions.get(expressionOffset++).evaluate(true);
            }

            Node old = Node.find
              ((Node) n.value, view.columns.get(columnOffset++),
               Compare.ColumnComparator);

            a.subtract
              (old == Node.Null ? a.aggregate.function.base() : old.value,
               values);
          }
        }

        if (view.query.hasAggregates
            && ((Integer) expressions.get(view.aggregateOffset).evaluate(true))
            != 0)
        {
          blaze(index).value = makeTree(stack, view.columns, expressions);
        } else {
          delete(index);
        }

        if (view.query.hasAggregates) {
          for (AggregateAdapter a: aggregates) {
            a.value = Compare.Undefined;
          }
        }
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }

    if (view.query.hasAggregates) {
      if (! sawSomething && view.query.groupingExpressions.isEmpty()) {
        // if there's no difference between the base and the fork, we
        // must synthesize a row containing the aggregates

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             expressions.get(view.primaryKeyOffset + i).evaluate(true),
             keyColumns.get(i).comparator);
        }

        Node n = blaze
          (i + Constants.IndexDataBodyDepth,
           expressions.get(view.primaryKeyOffset + i).evaluate(true),
           keyColumns.get(i).comparator);

        int columnOffset = view.aggregateOffset;
        int expressionOffset = view.aggregateExpressionOffset;
        for (AggregateAdapter a: aggregates) {
          a.value = a.aggregate.function.base();
        }
      
        n.value = makeTree(stack, view.columns, expressions);

        for (AggregateAdapter a: aggregates) {
          a.value = Compare.Undefined;
        }
      }

      // now, filter out rows which fail the query test if that test
      // uses an aggregate function

      // todo: only do this if the query test has aggregates

      qr.reset();

      while (true) {
        QueryResult.Type type = qr.nextRow();
        switch (type) {
        case End:
          return;

        case Inserted:
        case Deleted: {
          for (int i = 0; i < keyColumns.size(); ++i) {
            setKey
              (i + Constants.IndexDataBodyDepth,
               expressions.get(view.primaryKeyOffset + i).evaluate(true),
               keyColumns.get(i).comparator);
          }

          int index = keyColumns.size() - 1 + Constants.IndexDataBodyDepth;

          int columnOffset = view.aggregateOffset;
          for (AggregateAdapter a: aggregates) {
            a.value = Node.find
              ((Node) find(index).value,
               view.columns.get(columnOffset++),
               Compare.ColumnComparator).value;
          }

          // System.out.print("filter: ");
          // for (ColumnReferenceAdapter r: qr.expressionContext.columnReferences)
          // {
          //   System.out.print(r.column + ":" + r.value + " ");
          // }
          // System.out.println(": " + qr.test.evaluate(false));

          if (qr.test.evaluate(false) == Boolean.FALSE) {
            // todo: rather than delete this row from the view we
            // should actually just undo whatever operation we did in
            // the first pass, now that we know this query row should
            // not be included.  Note that we'll need to operate on a
            // copy of the view rather than the tentative one we just
            // built to make this work.

            delete(index);
          }

          for (AggregateAdapter a: aggregates) {
            a.value = Compare.Undefined;
          }
        } break;
      
        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
    }
  }

  private void checkStacks() {
    if (indexUpdateIterateStack == null) {
      indexUpdateIterateStack = new NodeStack();
      indexUpdateBaseStack = new NodeStack();
      indexUpdateForkStack = new NodeStack();
    }
  }

  private void updateIndexes() {
    updateIndexes(null);
  }

  private void updateIndexes(View viewToSkip) {
    if (dirtyIndexes && indexBase != result) {
      checkStacks();

      DiffIterator iterator = new DiffIterator
        (indexBase.root, indexUpdateBaseStack,
         result.root, indexUpdateForkStack,
         list(Interval.Unbounded).iterator(), false, Compare.TableComparator);

      DiffIterator.DiffPair pair = new DiffIterator.DiffPair();

      Set<View> viewSet = new HashSet();
      while (iterator.next(pair)) {
        if (pair.fork != null) {
          for (NodeIterator indexes = new NodeIterator
                 (indexUpdateIterateStack, Node.pathFind
                  (result.root, Constants.IndexTable, Compare.TableComparator,
                   Constants.IndexTable.primaryKey, Compare.IndexComparator,
                   pair.fork.key, Constants.TableColumn.comparator));
               indexes.hasNext();)
          {
            updateIndexTree
              ((Index) indexes.next().key, indexBase,
               indexUpdateBaseStack, indexUpdateForkStack);
          }

          for (NodeIterator views = new NodeIterator
                 (indexUpdateIterateStack, Node.pathFind
                  (result.root, Constants.ViewTable, Compare.TableComparator,
                   Constants.ViewTable.primaryKey, Compare.IndexComparator,
                   pair.fork.key, Constants.TableColumn.comparator));
               views.hasNext();)
          {
            View view = (View) views.next().key;
            if (! view.equals(viewToSkip)) {
              viewSet.add(view);
            }
          }
        }
      }

      for (View v: viewSet) {
        updateViewTree
          (v, indexBase, indexUpdateBaseStack, indexUpdateForkStack);
      }
    }

    dirtyIndexes = false;
    indexBase = result;
  }

  public void updateIndex(Index index) {
    if (! Compare.equal
        (index.table.primaryKey, index, Compare.IndexComparator))
    {
      updateIndexes();
    }
  }

  private void checkForeignKeys(ForeignKeyResolver resolver) {
    // todo: is there a performance problem with creating new
    // NodeStacks every time this method is called?  If so, are there
    // common cases were we can avoid creating them, or should we try
    // to recycle them somehow?

    ForeignKeys.checkForeignKeys
      (new NodeStack(), base, new NodeStack(), this, new NodeStack(),
       resolver, null);
  }

  public void prepareForUpdate(Table table) {
    // since we update non-primary-key indexes lazily, we may need to
    // freeze a copy of the last revision which contained up-to-date
    // indexes so we can do a diff later and use it to update them

    if (Constants.IndexTable.equals(table)
        || Constants.ViewTable.equals(table)
        || Node.pathFind
        (result.root, Constants.IndexTable, Compare.TableComparator,
         Constants.IndexTable.primaryKey, Compare.IndexComparator,
         table, Constants.TableColumn.comparator) != Node.Null
        || Node.pathFind
        (result.root, Constants.ViewTable, Compare.TableComparator,
         Constants.ViewTable.primaryKey, Compare.IndexComparator,
         table, Constants.TableColumn.comparator) != Node.Null)
    {
      dirtyIndexes = true;

      if (indexBase == result) {
        setToken(new Object());
      }
    }
  }

  private void pathInsert(Table table, Object ... path) {
    setKey(Constants.TableDataDepth, table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, table.primaryKey,
           Compare.IndexComparator);

    Node tree = Node.Null;
    Node.BlazeResult result = new Node.BlazeResult();
    List<Column<?>> columns = table.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      tree = Node.blaze
        (result, token, stack, tree, columns.get(i), Compare.ColumnComparator);

      result.node.value = path[i];

      Comparator comparator = columns.get(i).comparator;
      if (i == columns.size() - 1) {
        insertOrUpdate
          (Constants.IndexDataBodyDepth + i, path[i], comparator, tree);
      } else {
        setKey(Constants.IndexDataBodyDepth + i, path[i], comparator);
      }
    }
  }

  private void pathDelete(Table table, Object ... path) {
    setKey(Constants.TableDataDepth, table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, table.primaryKey,
           Compare.IndexComparator);

    List<Column<?>> columns = table.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      Comparator comparator = columns.get(i).comparator;
      if (i == columns.size() - 1) {
        deleteKey(Constants.IndexDataBodyDepth + i, path[i], comparator);
      } else {
        setKey(Constants.IndexDataBodyDepth + i, path[i], comparator);
      }
    }
  }

  private void addIndex(Index index)
  {
    if (index.equals(index.table.primaryKey)
        || Node.pathFind
        (result.root, Constants.IndexTable, Compare.TableComparator,
         Constants.IndexTable.primaryKey, Compare.IndexComparator,
         index.table, Constants.TableColumn.comparator,
         index, Constants.IndexColumn.comparator) != Node.Null)
    {
      // the specified index is already present -- ignore
      return;
    }

    // flush any changes out to the existing indexes, since we don't
    // want to get confused later when some indexes are up-to-date and
    // some aren't:
    updateIndexes();

    checkStacks();

    updateIndexTree
      (index, MyRevision.Empty, indexUpdateBaseStack, indexUpdateForkStack);

    pathInsert(Constants.IndexTable, index.table, index);
  }

  private void removeIndex(Index index)
  {
    if (index.equals(index.table.primaryKey)) {
      throw new IllegalArgumentException("cannot remove primary key");
    }

    pathDelete(Constants.IndexTable, index.table, index);

    setKey(Constants.TableDataDepth, index.table, Compare.TableComparator);
    deleteKey(Constants.IndexDataDepth, index, Compare.IndexComparator);
  }

  private void addView(View view)
  {
    addView(view, MyRevision.Empty);
  }

  void addView(final View view, MyRevision base)
  {
    final boolean isNew[] = new boolean[1];
    view.query.source.visit(new SourceVisitor() {
        public void visit(Source source) {
          if (source instanceof TableReference) {
            Table table = ((TableReference) source).table;

            if (Node.pathFind
                (result.root, Constants.ViewTable, Compare.TableComparator,
                 Constants.ViewTable.primaryKey, Compare.IndexComparator,
                 table, Constants.TableColumn.comparator,
                 view, Constants.ViewColumn.comparator) == Node.Null)
            {
              isNew[0] = true;
              insert(DuplicateKeyResolution.Throw, Constants.ViewTable, table,
                     view, Constants.ViewTableColumn, view.table);
            } else if (isNew[0]) {
              throw new RuntimeException
                ("view not indexed with all referenced tables");
            }
          }
        }
      });

    if (isNew[0]) {
      // flush any changes out to the existing indexes, since we don't
      // want to get confused later when some indexes are up-to-date and
      // some aren't:
      updateIndexes(view);

      checkStacks();

      add(Constants.ViewTableIndex);

      updateViewTree
        (view, base, indexUpdateBaseStack, indexUpdateForkStack);
    }
  }

  private void removeView(final View view)
  {
    view.query.source.visit(new SourceVisitor() {
        public void visit(Source source) {
          if (source instanceof TableReference) {
            Table table = ((TableReference) source).table;

            pathDelete(Constants.ViewTable, table, view);

            if (Node.pathFind
                (result.root, Constants.ViewTable, Compare.TableComparator)
                == Node.Null)
            {
              // the last view has been removed -- remove the index

              remove(Constants.ViewTableIndex);
            } else {
              dirtyIndexes = true;
            }
          }
        }
      });

    deleteKey(Constants.TableDataDepth, view.table, Compare.TableComparator);

    updateIndexes();
  }

  private void addForeignKey(ForeignKey constraint)
  {
    if (Node.pathFind
        (result.root, Constants.ForeignKeyTable, Compare.TableComparator,
         Constants.ForeignKeyTable.primaryKey, Compare.IndexComparator,
         constraint, Constants.ForeignKeyColumn.comparator) != Node.Null)
    {
      // the specified foreign key is already present -- ignore
      return;
    }

    insert(DuplicateKeyResolution.Throw, Constants.ForeignKeyTable, constraint,
           Constants.ForeignKeyRefererColumn, constraint.refererTable);

    insert(DuplicateKeyResolution.Throw, Constants.ForeignKeyTable, constraint,
           Constants.ForeignKeyReferentColumn, constraint.referentTable);

    add(Constants.ForeignKeyRefererIndex);
    add(Constants.ForeignKeyReferentIndex);

    dirtyIndexes = true;
    updateIndexes();
  }

  private void removeForeignKey(ForeignKey constraint)
  {
    pathDelete(Constants.ForeignKeyTable, constraint);

    if (Node.pathFind
        (result.root, Constants.ForeignKeyTable, Compare.TableComparator)
        == Node.Null)
    {
      // the last foreign key constraint has been removed -- remove
      // the indexes

      remove(Constants.ForeignKeyRefererIndex);
      remove(Constants.ForeignKeyReferentIndex);
    } else {
      dirtyIndexes = true;
      updateIndexes();
    }
  }

  private void doDelete(Object[] keys)
  {
    if (keys.length == 0) {
      deleteAll();
      return;
    }

    Table table = (Table) keys[0];

    if (keys.length == 1) {
      deleteKey(Constants.TableDataDepth, table, Compare.TableComparator);
      return;
    }

    prepareForUpdate(table);

    setKey(Constants.TableDataDepth, table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, table.primaryKey,
           Compare.IndexComparator);

    int i = 1;
    for (; i < keys.length - 1; ++i) {
      setKey(i - 1 + Constants.IndexDataBodyDepth, keys[i],
             table.primaryKey.columns.get(i - 1).comparator);
    }

    deleteKey(i - 1 + Constants.IndexDataBodyDepth, keys[i],
              keys.length > table.primaryKey.columns.size() + 1
              ? Compare.ColumnComparator
              : table.primaryKey.columns.get(i - 1).comparator);
  }

  private void insert(int depth,
                      List<Column<?>> columns,
                      Object[] path)
  {
    for (int i = 0; i < path.length; ++i) {
      blaze(depth, columns.get(i), Compare.ColumnComparator).value = path[i];
    }
  }

  private void insert(DuplicateKeyResolution duplicateKeyResolution,
                      Table table,
                      Column<?> column,
                      Object value,
                      Object[] path)
  {
    prepareForUpdate(table);

    setKey(Constants.TableDataDepth, table, Compare.TableComparator);
    setKey(Constants.IndexDataDepth, table.primaryKey,
           Compare.IndexComparator);

    for (int i = 0; i < path.length; ++i) {
      setKey(i + Constants.IndexDataBodyDepth, path[i],
             table.primaryKey.columns.get(i).comparator);
    }

    Node n;
    if (column == null) {
      n = blaze((path.length - 1) + Constants.IndexDataBodyDepth);
    } else {
      n = blaze
        (path.length + Constants.IndexDataBodyDepth, column,
         Compare.ColumnComparator);
    }

    if (n.value == Node.Null) {
      if (column != null) {
        n.value = value;
      }
      insert(path.length + Constants.IndexDataBodyDepth,
             table.primaryKey.columns, path);
    } else {
      switch (duplicateKeyResolution) {
      case Skip:
        break;

      case Overwrite:
        if (column != null) {
          n.value = value;
        }
        insert(path.length + Constants.IndexDataBodyDepth,
               table.primaryKey.columns, path);
        break;

      case Throw:
        throw new DuplicateKeyException();

      default:
        throw new RuntimeException
          ("unexpected resolution: " + duplicateKeyResolution);
      }
    }
  }

  private class MyTableBuilder implements TableBuilder {
    private class MyRowBuilder implements RowBuilder {
      private Object[] path;

      public MyRowBuilder() {
        path = new Object[3 + table.primaryKey.columns.size()];
      }

      public void init(Object[] keys) {
        if(path.length != keys.length + 3) {
          path = new Object[3 + keys.length];
        }
        path[0] = table;
        for(int i = 0; i < keys.length; i++) {
          path[i + 1] = keys[i];
        }
        insert(DuplicateKeyResolution.Overwrite, path, 0, path.length - 2);
      }
      
      public <T> RowBuilder update(Column<T> key,
                               T value)
      {
        path[path.length - 2] = key;
        path[path.length - 1] = value;
        insert(DuplicateKeyResolution.Overwrite, path);
        return this;
      }

      public RowBuilder delete(Column<?> key) {
        path[path.length - 2] = key;
        MyRevisionBuilder.this.delete(path, 0, path.length - 1);
        return this;
      }

      public RowBuilder row(Object ... key) {
        return up().row(key);
      }

      public TableBuilder table(Table table) {
        return up().table(table);
      }

      public Revision commit() {
        return commit(ForeignKeyResolvers.Restrict);
      }

      public Revision commit(ForeignKeyResolver foreignKeyResolver) {
        return up().commit(foreignKeyResolver);
      }

      private TableBuilder up() {
        MyTableBuilder.this.rowBuilder = this;
        return MyTableBuilder.this;
      }
    }

    private Table table;
    public MyRowBuilder rowBuilder;

    public MyTableBuilder() {
    }
    
    public void init(Table table) {
      this.table = table;
    }

    public RowBuilder row(Object ... key) {
      if(rowBuilder != null) {
        rowBuilder.init(key);
        MyRowBuilder ret = rowBuilder;
        rowBuilder = null;
        return ret;
      }
      MyRowBuilder ret = new MyRowBuilder();
      ret.init(key);
      return ret;
    }

    public TableBuilder key(Object ... key) {
      MyRowBuilder row = (MyRowBuilder) row(key);
      insert(DuplicateKeyResolution.Overwrite, row.path, 0, key.length + 1);
      return row.up(); // identically, return this (but up() can recycle the RowBuilder)
    }

    public TableBuilder delete(Object ... key) {
      MyRowBuilder row = (MyRowBuilder) row(key);
      MyRevisionBuilder.this.delete(row.path, 0, key.length + 1);
      return this;
    }

    public TableBuilder table(Table table) {
      return up().table(table);
    }

    public Revision commit() {
      return commit(ForeignKeyResolvers.Restrict);
    }

    public Revision commit(ForeignKeyResolver foreignKeyResolver) {
      return up().commit(foreignKeyResolver);
    }

    private RevisionBuilder up() {
      MyRevisionBuilder.this.tableBuilder = this;
      return MyRevisionBuilder.this;
    }
  }

  private MyTableBuilder tableBuilder = null;

  public TableBuilder table(Table table)
  {
    MyTableBuilder ret;
    if(tableBuilder == null) {
      ret = new MyTableBuilder();
    } else {
      ret = tableBuilder;
      tableBuilder = null;
    }
    ret.init(table);
    return ret;
  }

  public int apply(PatchTemplate template,
                   Object ... parameters)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      if (parameters.length != template.parameterCount()) {
        throw new IllegalArgumentException
          ("wrong number of parameters (expected "
           + template.parameterCount() + "; got "
           + parameters.length + ")");
      }

      return adapters.get
        (template.getClass()).apply(this, template, copy(parameters));
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void delete(Object[] path,
                     int pathOffset,
                     int pathLength)
  {
    Object[] myPath = new Object[pathLength];
    System.arraycopy(path, pathOffset, myPath, 0, pathLength);
    
    doDelete(myPath);
  }

  public void delete(Object ... path)
  {
    delete(path, 0, path.length);
  }

  public void insert(DuplicateKeyResolution duplicateKeyResolution,
                     Object[] path,
                     int pathOffset,
                     int pathLength)
  {
    Table table;
    try {
      table = (Table) path[pathOffset];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expected table as first path element");        
    }

    List<Column<?>> columns = table.primaryKey.columns;

    if (pathLength == columns.size() + 1) {
      Object[] myPath = new Object[columns.size()];
      System.arraycopy(path, pathOffset + 1, myPath, 0, myPath.length);

      insert(duplicateKeyResolution, table, null, null, myPath);
    } else if (pathLength == columns.size() + 3) {
      Column<?> column;
      try {
        column = (Column<?>) path[pathOffset + columns.size() + 1];
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expected column as second-to-last path element");        
      }

      Object value = path[pathOffset + columns.size() + 2];
      if (value != null && ! column.type.isInstance(value)) {
        throw new ClassCastException
          (value.getClass() + " cannot be cast to " + column.type);
      }

      Object[] myPath = new Object[columns.size()];
      for (int i = 0; i < myPath.length; ++i) {
        if (columns.get(i) == column) {
          throw new IllegalArgumentException
            ("cannot use insert to update a primary key column");        
        }
        myPath[i] = path[pathOffset + i + 1];
      }

      insert(duplicateKeyResolution, table, column, value, myPath);
    } else {
      Object[] myPath = new Object[pathLength];
      System.arraycopy(path, pathOffset, myPath, 0, pathLength);

      throw new IllegalArgumentException
        ("wrong number of parameters for primary key " + table.primaryKey
         + " path " + java.util.Arrays.toString(myPath));
    }

  }

  public void insert(DuplicateKeyResolution duplicateKeyResolution,
                     Object ... path)
  {
    insert(duplicateKeyResolution, path, 0, path.length);
  }

  public void add(Index index)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      addIndex(index);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void remove(Index index)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      removeIndex(index);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void add(View view)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      addView(view);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void remove(View view)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      removeView(view);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void add(ForeignKey constraint)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      addForeignKey(constraint);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public void remove(ForeignKey constraint)
  {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    try {
      removeForeignKey(constraint);
    } catch (RuntimeException e) {
      token = null;
      throw e;
    }
  }

  public boolean committed() {
    return token != null;
  }

  public Revision commit() {
    return commit(ForeignKeyResolvers.Restrict);
  }

  public Revision commit(ForeignKeyResolver foreignKeyResolver) {
    if (token == null) {
      return result;
    }

    updateIndexes();

    checkForeignKeys(foreignKeyResolver);

    token = null;

    return result;
  }

  private static MyRevision getRevision(Object token,
                                        MyRevision basis,
                                        Node root)
  {
    if (token == basis.token) {
      basis.root = root;
      return basis;
    } else {
      return new MyRevision(token, root);
    }
  }
}
