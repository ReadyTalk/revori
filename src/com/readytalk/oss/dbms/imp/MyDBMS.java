package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.DBMS;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.OutputStream;

public class MyDBMS implements DBMS {
  private static class Node <K, V> {
    public K key;
    public V value;
    public Node left;
    public Node right;
    public boolean red;
    
    public Node(Node<K, V> basis) {
      if (basis != null) {
        key = basis.key;
        value = basis.value;
        left = basis.left;
        right = basis.right;
        red = basis.red;
      }
    }
  }

  private static class MyColumn implements Column {
    public final ColumnType type;

    public MyColumn(ColumnType type) {
      this.type = type;
    }
  }

  private static class MyTable implements Table {
    public final Collection<MyColumn> columns;
    public final List<MyColumn> primaryKey;

    public MyTable(Collection<MyColumn> columns,
                   List<MyColumn> primaryKey)
    {
      this.columns = columns;
      this.primaryKey = primaryKey;
    }
  }

  private static class MyRevision implements Revision {
    public final Node<MyTable, Node> root;

    public MyRevision(Node<MyTable, Node> root) {
      this.root = root;
    }
  }

  private static class MyTableReference implements TableReference, MySource {
    public final MyTable table;

    public MyTableReference(MyTable table) {
      this.table = table;
    }

    public void visit(ExpressionVisitor visitor) {
      // ignore
    }
  }

  private static class ExpressionContext {
    public final Map<ParameterExpression, Integer> parameterIndexes;
    public final Object[] parameterValues;
    public final Map<MyColumnReference, Integer> columnIndexes;
    public final Object[] columnValues;

    public ExpressionContext
      (Map<ParameterExpression, Integer> parameterIndexes,
       Object[] parameterValues,
       Map<MyColumnReference, Integer> columnIndexes,
       Object[] columnValues)
    {
      this.parameterIndexes = parameterIndexes;
      this.parameterValues = parameterValues;
      this.columnIndexes = columnIndexes;
      this.columnValues = columnValues;
    }

    public Object get(ParameterExpression p) {
      return parameterValues[parameterIndexes.get(p)];
    }

    public Object get(MyColumnReference r) {
      return columnValues[columnIndexes.get(r)];
    }
  }

  private static interface MyExpression extends Expression {
    public Object evaluate(ExpressionContext context);
    public void visit(ExpressionVisitor visitor);
  }

  private static class MyColumnReference
    implements ColumnReference, MyExpression
  {
    public final MyTableReference tableReference;
    public final MyColumn column;

    public MyColumnReference(MyTableReference tableReference,
                             MyColumn column)
    {
      this.tableReference = tableReference;
      this.column = column;
    }

    public Object evaluate(ExpressionContext context) {
      return context.get(this);
    }

    public void visit(ExpressionVisitor visitor) {
      // ignore
    }
  }

  private static class ConstantExpression implements MyExpression {
    public final Object value;
    
    public ConstantExpression(Object value) {
      this.value = value;
    }

    public Object evaluate(ExpressionContext context) {
      return value;
    }

    public void visit(ExpressionVisitor visitor) {
      // ignore
    }
  }

  private static class ParameterExpression implements MyExpression {
    public Object evaluate(ExpressionContext context) {
      return context.get(this);
    }

    public void visit(ExpressionVisitor visitor) {
      visitor.visit(this);
    }
  }

  private static class EqualExpression implements MyExpression {
    public final MyExpression a;
    public final MyExpression b;
    
    public EqualExpression(MyExpression a,
                           MyExpression b)
    {
      this.a = a;
      this.b = b;
    }

    public Object evaluate(ExpressionContext context) {
      Object av = a.evaluate(context);
      Object bv = b.evaluate(context);

      if (av == null) {
        return bv == null;
      } else {
        return av.equals(bv);
      }
    }

    public void visit(ExpressionVisitor visitor) {
      a.visit(visitor);
      b.visit(visitor);
    }
  }

  private static interface MySource extends Source {
    public void visit(ExpressionVisitor visitor);
  }

  private static class Join implements MySource {
    public final JoinType type;
    public final MyTableReference left;
    public final MyTableReference right;
    public final MyExpression test;

    public Join(JoinType type,
                MyTableReference left,
                MyTableReference right,
                MyExpression test)
    {
      this.type = type;
      this.left = left;
      this.right = right;
      this.test = test;
    }

    public void visit(ExpressionVisitor visitor) {
      test.visit(visitor);
    }
  }

  private static class MyQueryTemplate implements QueryTemplate {
    public final Map<ParameterExpression, Integer> parameterIndexes;
    public final List<MyExpression> expressions;
    public final MySource source;
    public final MyExpression test;

    public MyQueryTemplate(Map<ParameterExpression, Integer> parameterIndexes,
                           List<MyExpression> expressions,
                           MySource source,
                           MyExpression test)
    {
      this.parameterIndexes = parameterIndexes;
      this.expressions = expressions;
      this.source = source;
      this.test = test;
    }
  }

  private static class MyQuery implements Query {
    public final MyQueryTemplate template;
    public final Object[] parameters;

    public MyQuery(MyQueryTemplate template,
                   Object[] parameters)
    {
      this.template = template;
      this.parameters = parameters;
    }
  }

  private static class AddedResultIterator implements ResultIterator {
    public final MyQueryResult result;

    public AddedResultIterator(MyQueryResult result) {
      this.result = result;
    }

    public boolean nextRow() {
      throw new UnsupportedOperationException("todo");
    }

    public Object nextItem() {
      throw new UnsupportedOperationException("todo");
    }
  }

  private static class RemovedResultIterator implements ResultIterator {
    public final MyQueryResult result;

    public RemovedResultIterator(MyQueryResult result) {
      this.result = result;
    }

    public boolean nextRow() {
      throw new UnsupportedOperationException("todo");
    }

    public Object nextItem() {
      throw new UnsupportedOperationException("todo");
    }
  }

  private static class MyQueryResult implements QueryResult {
    public final MyRevision base;
    public final MyRevision fork;
    public final MyQuery query;

    public MyQueryResult(MyRevision base,
                         MyRevision fork,
                         MyQuery query)
    {
      this.base = base;
      this.fork = fork;
      this.query = query;
    }

    public ResultIterator added() {
      return new AddedResultIterator(this);
    }

    public ResultIterator removed() {
      return new RemovedResultIterator(this);
    }
  }

  private static class MyPatchTemplate implements PatchTemplate {
    public final Map<ParameterExpression, Integer> parameterIndexes;

    public MyPatchTemplate(Map<ParameterExpression, Integer> parameterIndexes)
    {
      this.parameterIndexes = parameterIndexes;
    }
  }

  private static class InsertTemplate extends MyPatchTemplate {
    public final MyTable table;
    public final Map<MyColumn, MyExpression> values;

    public InsertTemplate(Map<ParameterExpression, Integer> parameterIndexes,
                          MyTable table,
                          Map<MyColumn, MyExpression> values)
    {
      super(parameterIndexes);
      this.table = table;
      this.values = values;
    }
  }

  private static class UpdateTemplate extends MyPatchTemplate {
    public final MyTable table;
    public final MyExpression test;
    public final Map<MyColumn, MyExpression> values;

    public UpdateTemplate(Map<ParameterExpression, Integer> parameterIndexes,
                          MyTable table,
                          MyExpression test,
                          Map<MyColumn, MyExpression> values)
    {
      super(parameterIndexes);
      this.table = table;
      this.test = test;
      this.values = values;
    }
  }

  private static class DeleteTemplate extends MyPatchTemplate {
    public final MyTable table;
    public final MyExpression test;

    public DeleteTemplate(Map<ParameterExpression, Integer> parameterIndexes,
                          MyTable table,
                          MyExpression test)
    {
      super(parameterIndexes);
      this.table = table;
      this.test = test;
    }
  }

  private interface MyPatch extends Patch { }

  private static class SinglePatch implements MyPatch {
    public final MyPatchTemplate template;
    public final Object[] parameters;

    public SinglePatch(MyPatchTemplate template,
                       Object[] parameters)
    {
      this.template = template;
      this.parameters = parameters;
    }
  }

  private static class SequencePatch implements MyPatch {
    public final List<MyPatch> sequence;

    public SequencePatch(List<MyPatch> sequence) {
      this.sequence = sequence;
    }
  }

  private static interface ExpressionVisitor {
    public void visit(Expression e);
  }

  private static class ParameterExpressionVisitor implements ExpressionVisitor
  {
    public final Map<ParameterExpression, Integer> parameterIndexes
      = new HashMap();
    public int nextIndex;

    public void visit(Expression e) {
      if (e instanceof ParameterExpression) {
        ParameterExpression pe = (ParameterExpression) e;
        if (parameterIndexes.containsKey(pe)) {
          throw new IllegalArgumentException
            ("duplicate parameter expressions");
        } else {
          parameterIndexes.put(pe, nextIndex++);
        }
      }
    }
  }

  private static final Node NullNode = new Node(null);

  static {
    NullNode.left = NullNode;
    NullNode.right = NullNode;
  }

  public Column column(ColumnType type) {
    return new MyColumn(type);
  }

  public Table table(Set<Column> columns,
                     List<Column> primaryKey)
  {
    Collection copyOfColumns = new ArrayList(columns);
    List copyOfPrimaryKey = new ArrayList(primaryKey);

    if (copyOfPrimaryKey.isEmpty()) {
      throw new IllegalArgumentException("primary key must not be empty");
    }

    for (Object c: copyOfColumns) {
      if (! (c instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    Set set = new HashSet();
    for (Object c: copyOfPrimaryKey) {
      if (set.contains(c)) {
        throw new IllegalArgumentException
          ("duplicate column in primary key");
      }

      set.add(c);

      if (! copyOfColumns.contains(c)) {
        throw new IllegalArgumentException
          ("primary key contains column not in column set");
      }
    }

    return new MyTable(copyOfColumns, copyOfPrimaryKey);
  }

  public Revision revision() {
    return new MyRevision(NullNode);
  }

  public TableReference tableReference(Table table) {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    return new MyTableReference(myTable);
  }

  public ColumnReference columnReference(TableReference tableReference,
                                         Column column)
  {
    MyTableReference myTableReference;
    try {
      myTableReference = (MyTableReference) tableReference;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("tableReference not created by this implementation");
    }

    MyColumn myColumn;
    try {
      myColumn = (MyColumn) column;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("column not created by this implementation");
    }

    return new MyColumnReference(myTableReference, myColumn);
  }

  public Expression constant(Object value) {
    return new ConstantExpression(value);
  }

  public Expression parameter() {
    return new ParameterExpression();
  }

  public Expression equal(Expression a,
                          Expression b)
  {
    MyExpression myA;
    MyExpression myB;
    try {
      myA = (MyExpression) a;
      myB = (MyExpression) b;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    return new EqualExpression(myA, myB);
  }

  public Source join(JoinType type,
                     TableReference left,
                     TableReference right,
                     Expression test)
  {
    MyTableReference myLeft;
    MyTableReference myRight;
    try {
      myLeft = (MyTableReference) left;
      myRight = (MyTableReference) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table reference not created by this implementation");
    }

    MyExpression myTest;
    try {
      myTest = (MyExpression) test;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    return new Join(type, myLeft, myRight, myTest);
  }

  public QueryTemplate queryTemplate(List<Expression> expressions,
                                     Source source,
                                     Expression test)
  {
    List copyOfExpressions = new ArrayList(expressions);

    ParameterExpressionVisitor visitor = new ParameterExpressionVisitor();

    for (Object expression: copyOfExpressions) {
      MyExpression myExpression;
      try {
        myExpression = (MyExpression) expression;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(visitor);
    }

    MySource mySource;
    try {
      mySource = (MySource) source;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("source not created by this implementation");        
    }

    mySource.visit(visitor);

    MyExpression myTest;
    try {
      myTest = (MyExpression) test;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    myTest.visit(visitor);

    return new MyQueryTemplate
      (visitor.parameterIndexes, copyOfExpressions, mySource, myTest);
  }

  public Query query(QueryTemplate template,
                     Object ... parameters)
  {
    MyQueryTemplate myTemplate;
    try {
      myTemplate = (MyQueryTemplate) template;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("query template not created by this implementation");
    }

    if (parameters.length != myTemplate.parameterIndexes.size()) {
      throw new IllegalArgumentException
        ("wrong number of parameters (expected "
         + myTemplate.parameterIndexes.size() + "; got "
         + parameters.length + ")");
    }

    return new MyQuery(myTemplate, copy(parameters));
  }

  public QueryResult diff(Revision a,
                          Revision b,
                          Query query)
  {
    MyRevision myA;
    MyRevision myB;
    try {
      myA = (MyRevision) a;
      myB = (MyRevision) b;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    MyQuery myQuery;
    try {
      myQuery = (MyQuery) query;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("query not created by this implementation");        
    }

    return new MyQueryResult(myA, myB, myQuery);
  }

  public PatchTemplate insertTemplate(Table table,
                                      Map<Column, Expression> values)
  {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    ParameterExpressionVisitor visitor = new ParameterExpressionVisitor();

    Map copyOfValues = new HashMap(values);
    Set set = new HashSet(myTable.columns);
    for (Object o: copyOfValues.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      if (! (entry.getKey() instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }

      if (! (set.contains(entry.getKey()))) {
        throw new IllegalArgumentException
          ("column not part of specified table");
      }

      set.remove(entry.getKey());

      MyExpression myExpression;
      try {
        myExpression = (MyExpression) entry.getValue();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(visitor);
    }

    if (set.size() != 0) {
      throw new IllegalArgumentException("not enough columns specified");
    }

    return new InsertTemplate(visitor.parameterIndexes, myTable, copyOfValues);
  }

  public PatchTemplate updateTemplate(Table table,
                                      Expression test,
                                      Map<Column, Expression> values)
  {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    MyExpression myTest;
    try {
      myTest = (MyExpression) test;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");        
    }

    ParameterExpressionVisitor visitor = new ParameterExpressionVisitor();

    myTest.visit(visitor);

    Map copyOfValues = new HashMap(values);
    Set set = new HashSet(myTable.columns);
    for (Object o: copyOfValues.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      if (! (entry.getKey() instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }

      if (! (set.contains(entry.getKey()))) {
        throw new IllegalArgumentException
          ("column not part of specified table");        
      }

      set.remove(entry.getKey());

      MyExpression myExpression;
      try {
        myExpression = (MyExpression) entry.getValue();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(visitor);
    }

    return new UpdateTemplate
      (visitor.parameterIndexes, myTable, myTest, copyOfValues);
  }

  public PatchTemplate deleteTemplate(Table table,
                                      Expression test)
  {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    MyExpression myTest;
    try {
      myTest = (MyExpression) test;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");        
    }

    ParameterExpressionVisitor visitor = new ParameterExpressionVisitor();

    myTest.visit(visitor);

    return new DeleteTemplate(visitor.parameterIndexes, myTable, myTest);
  }

  public Patch patch(PatchTemplate template,
                     Object ... parameters)
  {
    MyPatchTemplate myTemplate;
    try {
      myTemplate = (MyPatchTemplate) template;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch template not created by this implementation");        
    }

    if (parameters.length != myTemplate.parameterIndexes.size()) {
      throw new IllegalArgumentException
        ("wrong number of parameters (expected "
         + myTemplate.parameterIndexes.size() + "; got "
         + parameters.length + ")");
    }

    return new SinglePatch(myTemplate, copy(parameters));
  }

  public Patch sequence(List<Patch> sequence) {
    List copyOfSequence = new ArrayList(sequence);

    for (Object p: copyOfSequence) {
      if (! (p instanceof MyPatch)) {
        throw new IllegalArgumentException
          ("patch not created by this implementation");
      }
    }

    return new SequencePatch(copyOfSequence);
  }

  public Revision apply(Revision revision,
                        Patch patch)
  {
    MyRevision myRevision;
    try {
      myRevision = (MyRevision) revision;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    MyPatch myPatch;
    try {
      myPatch = (MyPatch) patch;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch not created by this implementation");
    }

    return new MyRevision(apply(myRevision.root, myPatch));
  }

  public Patch diff(Revision a,
                    Revision b)
  {
    MyRevision myA;
    MyRevision myB;
    try {
      myA = (MyRevision) a;
      myB = (MyRevision) b;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return diff(myA.root, myB.root);
  }

  public Revision merge(Revision base,
                        Revision forkA,
                        Revision forkB,
                        ConflictResolver conflictResolver)
  {
    MyRevision myBase;
    MyRevision myForkA;
    MyRevision myForkB;
    try {
      myBase = (MyRevision) base;
      myForkA = (MyRevision) forkA;
      myForkB = (MyRevision) forkB;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return new MyRevision
      (merge(myBase.root, myForkA.root, myForkB.root, conflictResolver));
  }

  public void write(Patch patch,
                    OutputStream out)
  {
    MyPatch myPatch;
    try {
      myPatch = (MyPatch) patch;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch not created by this implementation");
    }
    
    doWrite(myPatch, out);
  }

  public Patch readPatch(InputStream in) {
    throw new UnsupportedOperationException("todo");
  }

  private static Object[] copy(Object[] array) {
    Object[] copy = new Object[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  private static Node<MyTable, Node> apply(Node<MyTable, Node> a,
                                           MyPatch patch)
  {
    throw new UnsupportedOperationException("todo");
  }

  private static MyPatch diff(Node<MyTable, Node> a,
                              Node<MyTable, Node> b)
  {
    throw new UnsupportedOperationException("todo");
  }

  private static Node<MyTable, Node> merge(Node<MyTable, Node> base,
                                           Node<MyTable, Node> a,
                                           Node<MyTable, Node> b,
                                           ConflictResolver conflictResolver)
  {
    throw new UnsupportedOperationException("todo");
  }

  private static void doWrite(MyPatch patch,
                              OutputStream out)
  {
    throw new UnsupportedOperationException("todo");
  }
}
