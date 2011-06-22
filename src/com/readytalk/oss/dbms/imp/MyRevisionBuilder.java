package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.expect;
import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.copy;

import static com.readytalk.oss.dbms.ExpressionFactory.reference;
import static com.readytalk.oss.dbms.ExpressionFactory.isNull;
import static com.readytalk.oss.dbms.ExpressionFactory.and;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.SourceFactory.reference;
import static com.readytalk.oss.dbms.SourceFactory.leftJoin;

import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.DuplicateKeyException;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.UpdateTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyException;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.QueryTemplate;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;

class MyRevisionBuilder implements RevisionBuilder {
  private static final Map<Class, PatchTemplateAdapter> adapters
    = new HashMap();

  static {
    adapters.put(UpdateTemplate.class, new UpdateTemplateAdapter());
    adapters.put(InsertTemplate.class, new InsertTemplateAdapter());
    adapters.put(DeleteTemplate.class, new DeleteTemplateAdapter());
  }

  public Object token;
  public final NodeStack stack;
  public final Comparable[] keys;
  public final Node[] blazedRoots;
  public final Node[] blazedLeaves;
  public final Node[] found;
  public final Node.BlazeResult blazeResult = new Node.BlazeResult();
  public NodeStack indexUpdateBaseStack;
  public NodeStack indexUpdateForkStack;
  public MyRevision indexBase;
  public MyRevision result;
  public int max = -1;
  public boolean dirtyIndexes;

  public MyRevisionBuilder(Object token,
                           MyRevision result,
                           NodeStack stack)
  {
    this.token = token;
    this.indexBase = result;
    this.result = result;
    this.stack = stack;
    keys = new Comparable[Constants.MaxDepth + 1];
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

  public void setKey(int index, Comparable key) {
    if (max < index || ! Compare.equal(key, keys[index])) {
      max = index;
      keys[index] = key;
      found[index] = null;
      blazedLeaves[index] = null;
      blazedRoots[index + 1] = null;
    }
  }

  public Node blaze(int index, Comparable key) {
    setKey(index, key);
    return blaze(index);
  }

  public void insertOrUpdate(int index, Comparable key, Object value) {
    blaze(index, key).value = value;
  }

  public void delete(int index, Comparable key) {
    setKey(index, key);
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
        root = Node.delete(token, stack, result.root, keys[0]);

        if (root != result.root) {
          result = getRevision(token, result, root);
        }
      } else {
        Node original = find(index);
        Node originalRoot = (Node) find(index - 1).value;

        if (original == Node.Null) {
          throw new RuntimeException();
        } else if (original == originalRoot
                   && original.left == Node.Null
                   && original.right == Node.Null)
        {
          delete(index - 1);
        } else {
          root = Node.delete
            (token, stack, (Node) blaze(index - 1).value, keys[index]);
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
          n = Node.find(result.root, keys[0]);
          found[0] = n;
        } else {
          n = Node.find((Node) find(index - 1).value, keys[index]);
          found[index] = n;
        }
      }
    }
    return n;
  }

  private void deleteBlazed(int index) {
    blazedLeaves[index] = null;
    Node root = Node.delete(token, stack, blazedRoots[index], keys[index]);
    blazedRoots[index] = root;
    blazedLeaves[index] = null;
    if (root == null) {
      if (index == 0) {
        result.root = Node.delete(token, stack, result.root, keys[0]);
      } else {
        deleteBlazed(index - 1);
      }
    } else {
      if (index == 0) {
        result.root = root;
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
          (blazeResult, token, stack, result.root, keys[0]);

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
           keys[index]);

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

    List<Column> keyColumns = index.columns;

    TableIterator iterator
      = new TableIterator
      (reference(index.table), base, baseStack, result, forkStack,
       ConstantAdapter.True, new ExpressionContext(null), false);

    setKey(Constants.TableDataDepth, index.table);
    setKey(Constants.IndexDataDepth, index);

    boolean done = false;
    while (! done) {
      QueryResult.Type type = iterator.nextRow();
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             (Comparable) Node.find(tree, keyColumns.get(i)).value);
        }

        Node n = blaze
          (i + Constants.IndexDataBodyDepth,
           (Comparable) Node.find(tree, keyColumns.get(i)).value);

        expect(n.value == Node.Null);
      
        n.value = tree;
      } break;

      case Deleted: {
        Node tree = (Node) iterator.pair.base.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             (Comparable) Node.find(tree, keyColumns.get(i)).value);
        }

        delete
          (i + Constants.IndexDataBodyDepth,
           (Comparable) Node.find(tree, keyColumns.get(i)).value);            
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  private void updateIndexes() {
    if (dirtyIndexes && indexBase != result) {
      if (indexUpdateBaseStack == null) {
        indexUpdateBaseStack = new NodeStack();
        indexUpdateForkStack = new NodeStack();
      }

      DiffIterator iterator = new DiffIterator
        (indexBase.root, indexUpdateBaseStack,
         result.root, indexUpdateForkStack,
         list(Interval.Unbounded).iterator(), false);

      DiffIterator.DiffPair pair = new DiffIterator.DiffPair();

      while (iterator.next(pair)) {
        if (pair.fork != null) {
          for (NodeIterator indexes = new NodeIterator
                 (indexUpdateBaseStack, Node.pathFind
                  (result.root, Constants.IndexTable,
                   Constants.IndexTable.primaryKey, (Table) pair.fork.key));
               indexes.hasNext();)
          {
            updateIndexTree
              ((Index) indexes.next().key, indexBase,
               indexUpdateBaseStack, indexUpdateForkStack);
          }
        }
      }
    }

    dirtyIndexes = false;
    indexBase = result;
  }

  public void updateIndex(Index index) {
    if (! Compare.equal(index.table.primaryKey, index)) {
      updateIndexes();
    }
  }

  public void prepareForUpdate(Table table) {
    // since we update non-primary-key indexes lazily, we may need to
    // freeze a copy of the last revision which contained up-to-date
    // indexes so we can do a diff later and use it to update them

    if (Node.pathFind(result.root, Constants.IndexTable,
                      Constants.IndexTable.primaryKey, table) != Node.Null)
    {
      dirtyIndexes = true;

      if (indexBase == result) {
        setToken(new Object());
      }
    }
  }

  public void buildIndexTree(Index index)
  {
    TableIterator iterator = new TableIterator
      (reference(index.table), MyRevision.Empty, NodeStack.Null,
       result, new NodeStack(), ConstantAdapter.True,
       new ExpressionContext(null), false);

    setKey(Constants.TableDataDepth, index.table);
    setKey(Constants.IndexDataDepth, index);

    List<Column> keyColumns = index.columns;

    boolean done = false;
    while (! done) {
      QueryResult.Type type = iterator.nextRow();
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          setKey
            (i + Constants.IndexDataBodyDepth,
             (Comparable) Node.find(tree, keyColumns.get(i)).value);
        }

        insertOrUpdate
          (i + Constants.IndexDataBodyDepth,
           (Comparable) Node.find(tree, keyColumns.get(i)).value, tree);
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  private void pathInsert(Table table, Comparable ... path) {
    setKey(Constants.TableDataDepth, table);
    setKey(Constants.IndexDataDepth, table.primaryKey);

    Node tree = Node.Null;
    Node.BlazeResult result = new Node.BlazeResult();
    List<Column> columns = table.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      tree = Node.blaze(result, token, stack, tree, columns.get(i));
      result.node.value = path[i];

      if (i == columns.size() - 1) {
        insertOrUpdate(Constants.IndexDataBodyDepth + i, path[i], tree);
      } else {
        setKey(Constants.IndexDataBodyDepth + i, path[i]);
      }
    }
  }

  private void pathDelete(Table table, Comparable ... path) {
    setKey(Constants.TableDataDepth, table);
    setKey(Constants.IndexDataDepth, table.primaryKey);

    List<Column> columns = table.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      if (i == columns.size() - 1) {
        delete(Constants.IndexDataBodyDepth + i, path[i]);
      } else {
        setKey(Constants.IndexDataBodyDepth + i, path[i]);
      }
    }
  }

  private void addIndex(Index index)
  {
    if (index.equals(index.table.primaryKey)
        || Node.pathFind
        (result.root, Constants.IndexTable, Constants.IndexTable.primaryKey,
         index.table, index) != Node.Null)
    {
      // the specified index is already present -- ignore
      return;
    }

    // flush any changes out to the existing indexes, since we don't
    // want to get confused later when some indexes are up-to-date and
    // some aren't:
    updateIndexes();

    buildIndexTree(index);

    pathInsert(Constants.IndexTable, index.table, index);
  }

  private void removeIndex(Index index)
  {
    if (index.equals(index.table.primaryKey)) {
      throw new IllegalArgumentException("cannot remove primary key");
    }

    pathDelete(Constants.IndexTable, index.table, index);

    setKey(Constants.TableDataDepth, index.table);
    delete(Constants.IndexDataDepth, index);
  }

  private void checkForeignKey(Revision base, ForeignKey constraint) {
    TableReference referer = reference(constraint.refererTable);
    TableReference referent = reference(constraint.referentTable);

    Expression test = isNull
      (reference
       (referent, constraint.referentTable.primaryKey.columns.get(0)));

    Iterator<Column> refererIterator = constraint.refererColumns.iterator();
    Iterator<Column> referentIterator = constraint.referentColumns.iterator();
    while (refererIterator.hasNext()) {
      test = and(test, equal(reference(referer, refererIterator.next()),
                             reference(referent, referentIterator.next())));
    }

    if (base.diff
        (result, new QueryTemplate
         ((List<Expression>) (List) Collections.emptyList(),
          leftJoin(referer, referent), test)).nextRow()
        != QueryResult.Type.End)
    {
      throw new ForeignKeyException
        ("one or more rows exist in " + constraint.refererTable
         + " with no corresponding row in " + constraint.referentTable
         + " for " + constraint);
    }
  }

  private void addForeignKey(ForeignKey constraint)
  {
    if (Node.pathFind
        (result.root, Constants.ForeignKeyTable,
         Constants.ForeignKeyTable.primaryKey, constraint) != Node.Null)
    {
      // the specified foreign key is already present -- ignore
      return;
    }

    checkForeignKey(MyRevision.Empty, constraint);

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

    if (Node.pathFind(result.root, Constants.ForeignKeyTable) == Node.Null) {
      // the last foreign key constraint has been removed -- remove
      // the indexes

      remove(Constants.ForeignKeyRefererIndex);
      remove(Constants.ForeignKeyReferentIndex);
    } else {
      dirtyIndexes = true;
      updateIndexes();
    }
  }

  private void delete(Comparable[] keys)
  {
    if (keys.length == 0) {
      deleteAll();
      return;
    }

    Table table = (Table) keys[0];

    if (keys.length == 1) {
      delete(Constants.TableDataDepth, table);
      return;
    }

    prepareForUpdate(table);

    setKey(Constants.TableDataDepth, table);
    setKey(Constants.IndexDataDepth, table.primaryKey);

    int i = 1;
    for (; i < keys.length - 1; ++i) {
      setKey(i - 1 + Constants.IndexDataBodyDepth, keys[i]);
    }

    delete(i - 1 + Constants.IndexDataBodyDepth, keys[i]);
  }

  private void insert(DuplicateKeyResolution duplicateKeyResolution,
                      Table table,
                      Column column,
                      Object value,
                      Comparable[] path)
  {
    prepareForUpdate(table);

    setKey(Constants.TableDataDepth, table);
    setKey(Constants.IndexDataDepth, table.primaryKey);

    for (int i = 0; i < path.length; ++i) {
      setKey(i + Constants.IndexDataBodyDepth, path[i]);
    }

    Node n = blaze(path.length + Constants.IndexDataBodyDepth, column);

    if (n.value == Node.Null) {
      n.value = value;
    } else {
      switch (duplicateKeyResolution) {
      case Skip:
        break;

      case Overwrite:
        n.value = value;
        break;

      case Throw:
        throw new DuplicateKeyException();

      default:
        throw new RuntimeException
          ("unexpected resolution: " + duplicateKeyResolution);
      }
    }
  }

  public List<RefererForeignKeyAdapter> getRefererForeignKeyAdapters
    (Table table)
  {
    if (indexUpdateBaseStack == null) {
      indexUpdateBaseStack = new NodeStack();
      indexUpdateForkStack = new NodeStack();
    }

    List<RefererForeignKeyAdapter> list = new ArrayList();

    for (NodeIterator keys = new NodeIterator
           (indexUpdateBaseStack, Node.pathFind
            (result.root, Constants.ForeignKeyTable,
             Constants.ForeignKeyRefererIndex, table));
         keys.hasNext();)
    {
      list.add(new RefererForeignKeyAdapter((ForeignKey) keys.next().key));
    }

    return list;
  }

  public List<ReferentForeignKeyAdapter> getReferentForeignKeyAdapters
    (Table table)
  {
    if (indexUpdateBaseStack == null) {
      indexUpdateBaseStack = new NodeStack();
      indexUpdateForkStack = new NodeStack();
    }

    List<ReferentForeignKeyAdapter> list = new ArrayList();

    for (NodeIterator keys = new NodeIterator
           (indexUpdateBaseStack, Node.pathFind
            (result.root, Constants.ForeignKeyTable,
             Constants.ForeignKeyReferentIndex, table));
         keys.hasNext();)
    {
      list.add(new ReferentForeignKeyAdapter((ForeignKey) keys.next().key));
    }

    return list;
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
    Comparable[] myPath = new Comparable[pathLength];
    for (int i = 0; i < pathLength; ++i) {
      myPath[i] = (Comparable) path[pathOffset + i];
    }
    
    delete(myPath);
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

    List<Column> columns = table.primaryKey.columns;

    if (pathLength != columns.size() + 3) {
      throw new IllegalArgumentException
        ("wrong number of parameters for primary key");
    }

    Column column;
    try {
      column = (Column) path[pathOffset + columns.size() + 1];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expected column as second-to-last path element");        
    }

    Comparable[] myPath = new Comparable[columns.size()];
    for (int i = 0; i < myPath.length; ++i) {
      Comparable c = (Comparable) path[pathOffset + i + 1];
      if (columns.get(i) == column) {
        throw new IllegalArgumentException
          ("cannot use insert to update a primary key column");        
      }
      myPath[i] = c;
    }

    Object value = path[pathOffset + columns.size() + 2];
    if (value != null && ! column.type.isInstance(value)) {
      throw new ClassCastException
        (value.getClass() + " cannot be cast to " + column.type);
    }

    insert(duplicateKeyResolution, table, column, value, myPath);
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

  public Revision commit() {
    if (token == null) {
      throw new IllegalStateException("builder already committed");
    }

    updateIndexes();

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
