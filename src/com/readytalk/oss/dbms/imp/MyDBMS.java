package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.imp.Util.list;

import com.readytalk.oss.dbms.DBMS;

import java.util.Collections;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class MyDBMS implements DBMS {
  private static final boolean Debug = true;

  private static final boolean VerboseDiff = false;
  private static final boolean VerboseTest = false;

  private static final int TableDataDepth = 0;
  private static final int IndexDataDepth = 1;
  private static final int IndexDataBodyDepth = 2;
  private static final int MaxIndexDataBodyDepth = 8;
  private static final int MaxDepth
    = IndexDataBodyDepth + MaxIndexDataBodyDepth;
  private static final int NodeStackSize = 64;

  private static final Comparable Undefined = new Comparable() {
      public int compareTo(Object o) {
        throw new UnsupportedOperationException();
      }
      
      public String toString() {
        return "undefined";
      }
    };

  private static final LiveExpression UndefinedExpression
    = new Constant(Undefined);

  private static final Comparable Dummy = new Comparable() {
      public int compareTo(Object o) {
        throw new UnsupportedOperationException();
      }
      
      public String toString() {
        return "dummy";
      }
    };

  private static final Interval UnboundedInterval
    = new Interval(UndefinedExpression, UndefinedExpression);

  private static final EvaluatedInterval UnboundedEvaluatedInterval
    = new EvaluatedInterval(Undefined, Undefined);

  private static final Unknown UnknownInterval = new Unknown();

  private static final AtomicInteger nextId = new AtomicInteger();

  private static final Node NullNode = new Node(new Object(), null);

  private static final MyRevision EmptyRevision
    = new MyRevision(new Object(), NullNode);

  private static final NodeStack NullNodeStack = new NodeStack((Node[]) null);

  private static final Constant TrueConstant = new Constant(true);

  private static final MyColumn TableColumn = new MyColumn(MyTable.class);
  private static final MyColumn IndexKeyColumn
    = new MyColumn(IndexKey.class);
  private static final MyColumn IndexColumn
    = new MyColumn(MyIndex.class);

  private static final MyTable IndexTable
    = new MyTable(list(TableColumn, IndexKeyColumn, IndexColumn));

  static {
    NullNode.left = NullNode;
    NullNode.right = NullNode;
    NullNode.value = NullNode;
  }

  private static void expect(boolean v) {
    if (Debug && ! v) {
      throw new RuntimeException();
    }
  }

  private static int nextId() {
    return nextId.getAndIncrement();
  }

  private enum BoundType {
    Inclusive, Exclusive;

    static {
      Inclusive.opposite = Exclusive;
      Exclusive.opposite = Inclusive;
    }

    public BoundType opposite;
  }

  private static class Node {
    public final Object token;
    public Comparable key;
    public Object value;
    public Node left;
    public Node right;
    public boolean red;
    
    public Node(Object token, Node basis) {
      this.token = token;

      if (basis != null) {
        key = basis.key;
        value = basis.value;
        left = basis.left;
        right = basis.right;
        red = basis.red;
      }
    }
  }

  private static Node getNode(Object token, Node basis) {
    if (basis.token == token) {
      return basis;
    } else {
      return new Node(token, basis);
    }
  }

  private static class NodeStack {
    public final Node[] array;
    public final int base;
    public final NodeStack next;
    public NodeStack previous;
    public Node top;
    public int index;

    public NodeStack(Node[] array) {
      this.array = array;
      this.base = 0;
      this.next = null;
    }

    public NodeStack() {
      this(new Node[NodeStackSize]);
    }

    public NodeStack(NodeStack basis) {
      expect(basis.array == null || basis.previous == null);

      this.array = basis.array;
      this.base = basis.index;
      this.index = this.base;
      this.next = basis;

      if (basis.array != null) {
        basis.previous = this;
      }
    }
  }

  private static NodeStack popStack(NodeStack s) {
    expect(s.previous == null);
    expect(s.array == null || s.next.previous == s);

    s = s.next;
    s.previous = null;

    return s;
  }

  private static void push(NodeStack s, Node n) {
    if (s.top != null) {
      s.array[s.index++] = s.top;
    }
    s.top = n;
  }

  private static Node peek(NodeStack s, int depth) {
    expect(s.index - depth > s.base);

    return s.array[s.index - depth - 1];
  }

  private static Node peek(NodeStack s) {
    return peek(s, 0);
  }

  private static void pop(NodeStack s, int count) {
    expect(count > 0);
    expect(s.top != null);

    if (s.index - count < s.base) {
      expect(s.index - count == s.base - 1);

      s.index -= count - 1;
      s.top = null;
    } else {
      expect(s.index - count >= s.base);

      s.index -= count;
      s.top = s.array[s.index];
    }
  }

  private static void pop(NodeStack s) {
    pop(s, 1);
  }

  private static void clear(NodeStack s) {
    s.top = null;
    s.index = s.base;
  }

  private static class MyColumn implements Column, Comparable<MyColumn> {
    public final Class type;
    public final int id;

    public MyColumn(Class type) {
      this.type = type;
      this.id = nextId();
    }

    public int compareTo(MyColumn o) {
      return id - o.id;
    }
      
    public String toString() {
      return "column[" + id + "]";
    }
  }

  private static class IndexKey implements Comparable<IndexKey> {
    public final MyIndex index;

    public IndexKey(MyIndex index) {
      this.index = index;
    }

    public int compareTo(IndexKey o) {
      if (this == o) {
        return 0;
      }

      int d = index.table.compareTo(o.index.table);
      if (d != 0) {
        return d;
      }

      d = index.columns.size() - o.index.columns.size();
      if (d != 0) {
        return d;
      }

      Iterator<MyColumn> mine = index.columns.iterator();
      Iterator<MyColumn> other = o.index.columns.iterator();
      while (mine.hasNext()) {
        d = mine.next().compareTo(other.next());
        if (d != 0) {
          return d;
        }
      }
      return 0;
    }

    public boolean equal(Object o) {
      return o instanceof IndexKey && compareTo((IndexKey) o) == 0;
    }

    public int hashCode() {
      int h = index.table.hashCode();
      for (MyColumn c: index.columns) {
        h ^= c.hashCode();
      }
      return h;
    }
      
    public String toString() {
      return "indexKey[" + index.table + " " + index.columns + "]";
    }
  }

  private static class MyIndex implements Index, Comparable<MyIndex> {
    public final MyTable table;
    public final List<MyColumn> columns;
    public final IndexKey key = new IndexKey(this);    
    public final int id;

    public MyIndex(MyTable table,
                   List<MyColumn> columns)
    {
      this.table = table;
      this.columns = columns;
      this.id = nextId();
    }

    public int compareTo(MyIndex o) {
      return id - o.id;
    }
      
    public String toString() {
      return "index[" + id + " " + table + " " + columns + "]";
    }
  }

  private static void buildIndexTree(MyPatchContext context,
                                     MyIndex index)
  {
    MyTableReference.MySourceIterator iterator = new MyTableReference
      (index.table).iterator
      (EmptyRevision, NullNodeStack, context.result, new NodeStack(),
       TrueConstant, new ExpressionContext(null), false);

    context.setKey(TableDataDepth, index.table);
    context.setKey(IndexDataDepth, index.key);

    List<MyColumn> keyColumns = index.columns;

    boolean done = false;
    while (! done) {
      ResultType type = iterator.nextRow();
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          context.setKey
            (i + IndexDataBodyDepth,
             (Comparable) find(tree, keyColumns.get(i)).value);
        }

        context.insertOrUpdate
          (i + IndexDataBodyDepth,
           (Comparable) find(tree, keyColumns.get(i)).value, tree);          
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  private static void addIndex(MyPatchContext context,
                               MyIndex index)
  {
    if ((! index.key.equals(index.table.primaryKey.key))
        && pathFind(context.result.root, IndexTable, IndexTable.primaryKey.key,
                    index.table, index.key)
        == NullNode)
    {
      // flush any changes out to the existing indexes, since we don't
      // want to get confused later when some indexes are up-to-date
      // and some aren't:
      updateIndexes(context);

      buildIndexTree(context, index);
    }
    
    context.setKey(TableDataDepth, IndexTable);
    context.setKey(IndexDataDepth, IndexTable.primaryKey.key);

    Node tree = NullNode;
    BlazeResult result = new BlazeResult();
    Comparable[] values = new Comparable[] { index.table, index.key, index };
    List<MyColumn> columns = IndexTable.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      tree = blaze
        (result, context.token, context.stack, tree, columns.get(i));
      result.node.value = values[i];

      if (i == columns.size() - 1) {
        context.insertOrUpdate(IndexDataBodyDepth + i, values[i], tree);
      } else {
        context.setKey(IndexDataBodyDepth + i, values[i]);
      }
    }
  }

  private static void removeIndex(MyPatchContext context,
                                  MyIndex index)
  {
    context.setKey(TableDataDepth, IndexTable);
    context.setKey(IndexDataDepth, IndexTable.primaryKey.key);

    Comparable[] values = new Comparable[] { index.table, index.key, index };
    List<MyColumn> columns = IndexTable.primaryKey.columns;
    for (int i = 0; i < columns.size(); ++i) {
      if (i == columns.size() - 1) {
        context.delete(IndexDataBodyDepth + i, values[i]);
      } else {
        context.setKey(IndexDataBodyDepth + i, values[i]);
      }
    }

    if ((! index.key.equals(index.table.primaryKey.key))
        && pathFind(context.result.root, IndexTable, IndexTable.primaryKey.key,
                    index.table, index.key)
        == NullNode)
    {
      context.setKey(TableDataDepth, index.table);
      context.delete(IndexDataDepth, index.key);
    }
  }

  private static class MyTable implements Table, Comparable<MyTable> {
    public final MyIndex primaryKey;
    public final int id;

    public MyTable(List<MyColumn> primaryKey) {
      this.primaryKey = new MyIndex(this, primaryKey);
      this.id = nextId();
    }

    public int compareTo(MyTable o) {
      return id - o.id;
    }
      
    public String toString() {
      return "table[" + id + "]";
    }
  }

  private static class MyRevision implements Revision {
    public final Object token;
    public Node root;

    public MyRevision(Object token, Node root) {
      this.token = token;
      this.root = root;
    }
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

  private static class EvaluatedInterval {
    public final Comparable low;
    public final BoundType lowBoundType;
    public final Comparable high;
    public final BoundType highBoundType;

    public EvaluatedInterval(Comparable low,
                             BoundType lowBoundType,
                             Comparable high,
                             BoundType highBoundType)
    {
      this.low = low;
      this.lowBoundType = lowBoundType;
      this.high = high;
      this.highBoundType = highBoundType;
    }

    public EvaluatedInterval(Comparable low,
                             Comparable high)
    {
      this(low, BoundType.Inclusive, high, BoundType.Inclusive);
    }
  }

  private static interface Scan {
    public boolean isUseful();
    public boolean isSpecific();
    public boolean isUnknown();
    public List<EvaluatedInterval> evaluate();
  }

  private static class Unknown implements Scan {
    public boolean isUseful() {
      return false;
    }

    public boolean isSpecific() {
      return false;
    }

    public boolean isUnknown() {
      return true;
    }

    public List<EvaluatedInterval> evaluate() {
      return list(UnboundedEvaluatedInterval);
    }
  }

  private static class Interval implements Scan {
    public final LiveExpression low;
    public final BoundType lowBoundType;
    public final LiveExpression high;
    public final BoundType highBoundType;

    public Interval(LiveExpression low,
                    BoundType lowBoundType,
                    LiveExpression high,
                    BoundType highBoundType)
    {
      this.low = low;
      this.lowBoundType = lowBoundType;
      this.high = high;
      this.highBoundType = highBoundType;
    }

    public Interval(LiveExpression low,
                    LiveExpression high)
    {
      this(low, BoundType.Inclusive, high, BoundType.Inclusive);
    }

    public boolean isUseful() {
      return low.evaluate(false) != Undefined 
        || high.evaluate(false) != Undefined;
    }

    public boolean isUnknown() {
      return false;
    }

    public boolean isSpecific() {
      return low.evaluate(false) != Undefined
        && high.evaluate(false) != Undefined;
    }

    public List<EvaluatedInterval> evaluate() {
      return list
        (new EvaluatedInterval
         ((Comparable) low.evaluate(false), lowBoundType,
          (Comparable) high.evaluate(false), highBoundType));
    }
  }

  private static int compare(Comparable left,
                             Comparable right)
  {
    if (left == right) {
      return 0;
    } else if (left == Dummy) {
      return -1;
    } else if (right == Dummy) {
      return 1;
    } else {
      return ((Comparable) left).compareTo(right);
    }
  }

  private static boolean equal(Object left,
                               Object right)
  {
    return left == right || (left != null && left.equals(right));
  }

  private static int compare(Comparable left,
                             boolean leftHigh,
                             Comparable right,
                             boolean rightHigh)
  {
    if (left == Undefined) {
      if (right == Undefined) {
        if (leftHigh) {
          if (rightHigh) {
            return 0;
          } else {
            return 1;
          }
        } else if (rightHigh) {
          return -1;
        } else {
          return 0;
        }
      } else if (leftHigh) {
        return 1;
      } else {
        return -1;
      }
    } else if (right == Undefined) {
      if (rightHigh) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return compare(left, right);
    }
  }

  private static int compare(EvaluatedInterval left,
                             EvaluatedInterval right)
  {
    int leftHighRightHigh = compare(left.high, true, right.high, true);
    if (leftHighRightHigh > 0) {
      int leftLowRightHigh = compare(left.low, false, right.high, true);
      if (leftLowRightHigh < 0
          || left.lowBoundType == BoundType.Inclusive
          || right.lowBoundType == BoundType.Inclusive)
      {
        return 1;
      } else {
        return 2;
      }
    } else if (leftHighRightHigh < 0) {
      int rightLowLeftHigh = compare(right.low, false, left.high, true);
      if (rightLowLeftHigh < 0
          || right.lowBoundType == BoundType.Inclusive
          || left.lowBoundType == BoundType.Inclusive)
      {
        return -1;
      } else {
        return -2;
      }
    } else {
      if (left.highBoundType == BoundType.Inclusive) {
        if (right.highBoundType == BoundType.Inclusive) {
          return 0;
        } else {
          return 1;
        }
      } else if (right.highBoundType == BoundType.Inclusive) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  private static int compare(Comparable left,
                             Comparable right,
                             boolean rightHigh)
  {
    if (right == Undefined) {
      if (rightHigh) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return compare(left, right);
    }
  }

  private static int compare(Comparable left,
                             Comparable right,
                             BoundType rightBoundType,
                             boolean rightHigh)
  {
    int difference = compare(left, right, rightHigh);
    if (difference == 0) {
      if (rightBoundType == BoundType.Exclusive) {
        if (rightHigh) {
          return 1;
        } else {
          return -1;
        }
      } else {
        return 0;
      }
    } else {
      return difference;
    }
  }

  private static EvaluatedInterval intersection(EvaluatedInterval left,
                                                EvaluatedInterval right)
  {
    Comparable low;
    BoundType lowBoundType;
    int lowDifference = compare(left.low, false, right.low, false);
    if (lowDifference > 0) {
      low = left.low;
      lowBoundType = left.lowBoundType;
    } else if (lowDifference < 0) {
      low = right.low;
      lowBoundType = right.lowBoundType;
    } else {
      low = left.low;
      lowBoundType = (left.lowBoundType == BoundType.Exclusive
                      || right.lowBoundType == BoundType.Exclusive
                      ? BoundType.Exclusive : BoundType.Inclusive);
    }

    Comparable high;
    BoundType highBoundType;
    int highDifference = compare(left.high, true, right.high, true);
    if (highDifference > 0) {
      high = right.high;
      highBoundType = right.highBoundType;
    } else if (highDifference < 0) {
      high = left.high;
      highBoundType = left.highBoundType;
    } else {
      high = left.high;
      highBoundType = (left.highBoundType == BoundType.Exclusive
                      || right.highBoundType == BoundType.Exclusive
                      ? BoundType.Exclusive : BoundType.Inclusive);
    }

    return new EvaluatedInterval(low, lowBoundType, high, highBoundType);
  }

  private static EvaluatedInterval union(EvaluatedInterval left,
                                         EvaluatedInterval right)
  {
    Comparable low;
    BoundType lowBoundType;
    int lowDifference = compare(left.low, false, right.low, false);
    if (lowDifference > 0) {
      low = right.low;
      lowBoundType = right.lowBoundType;
    } else if (lowDifference < 0) {
      low = left.low;
      lowBoundType = left.lowBoundType;
    } else {
      low = left.low;
      lowBoundType = (left.lowBoundType == BoundType.Inclusive
                      || right.lowBoundType == BoundType.Inclusive
                      ? BoundType.Inclusive : BoundType.Exclusive);
    }

    Comparable high;
    BoundType highBoundType;
    int highDifference = compare(left.high, true, right.high, true);
    if (highDifference > 0) {
      high = left.high;
      highBoundType = left.highBoundType;
    } else if (highDifference < 0) {
      high = right.high;
      highBoundType = right.highBoundType;
    } else {
      high = left.high;
      highBoundType = (left.lowBoundType == BoundType.Inclusive
                       || right.lowBoundType == BoundType.Inclusive
                       ? BoundType.Inclusive : BoundType.Exclusive);
    }

    return new EvaluatedInterval(low, lowBoundType, high, highBoundType);
  }

  private static class Intersection implements Scan {
    public final Scan left;
    public final Scan right;

    public Intersection(Scan left,
                        Scan right)
    {
      this.left = left;
      this.right = right;
    }

    public boolean isUseful() {
      return left.isUseful() || right.isUseful();
    }

    public boolean isSpecific() {
      return left.isSpecific() || right.isSpecific();
    }

    public boolean isUnknown() {
      return left.isUnknown() && right.isUnknown();
    }

    public List<EvaluatedInterval> evaluate() {
      Iterator<EvaluatedInterval> leftIterator = left.evaluate().iterator();
      Iterator<EvaluatedInterval> rightIterator = right.evaluate().iterator();
      List<EvaluatedInterval> result = new ArrayList();

      int d = 0;
      EvaluatedInterval leftItem = null;
      EvaluatedInterval rightItem = null;
      while (leftIterator.hasNext() && rightIterator.hasNext()) {
        if (d < 0) {
          leftItem = leftIterator.next();
        } else if (d > 0) {
          rightItem = rightIterator.next();
        } else {
          leftItem = leftIterator.next();
          rightItem = rightIterator.next();
        }

        d = compare(leftItem, rightItem);
        if (d >= -1 && d <= 1) {
          result.add(intersection(leftItem, rightItem));
        }
      }

      return result;
    }
  }

  private static class Union implements Scan {
    public final Scan left;
    public final Scan right;

    public Union(Scan left,
                 Scan right)
    {
      this.left = left;
      this.right = right;
    }

    public boolean isUseful() {
      return left.isUseful() && right.isUseful();
    }

    public boolean isSpecific() {
      return left.isSpecific() && right.isSpecific();
    }

    public boolean isUnknown() {
      return left.isUnknown() || right.isUnknown();
    }

    public List<EvaluatedInterval> evaluate() {
      Iterator<EvaluatedInterval> leftIterator = left.evaluate().iterator();
      Iterator<EvaluatedInterval> rightIterator = right.evaluate().iterator();
      List<EvaluatedInterval> result = new ArrayList();

      int d = 0;
      EvaluatedInterval leftItem = null;
      EvaluatedInterval rightItem = null;
      while (leftIterator.hasNext() && rightIterator.hasNext()) {
        if (d < 0) {
          leftItem = leftIterator.next();
        } else if (d > 0) {
          rightItem = rightIterator.next();
        } else {
          leftItem = leftIterator.next();
          rightItem = rightIterator.next();
        }

        d = compare(leftItem, rightItem);
        if (d < -1) {
          result.add(leftItem);
          result.add(rightItem);
        } else if (d > 1) {
          result.add(rightItem);
          result.add(leftItem);
        } else {
          result.add(union(leftItem, rightItem));
        }
      }

      while (leftIterator.hasNext()) {
        result.add(leftIterator.next());
      }

      while (rightIterator.hasNext()) {
        result.add(rightIterator.next());
      }

      return result;
    }
  }

  private static class Negation implements Scan {
    public final Scan operand;

    public Negation(Scan operand) {
      this.operand = operand;
    }

    public boolean isUseful() {
      return isSpecific();
    }

    public boolean isSpecific() {
      return ! (operand.isUnknown() || operand.isSpecific());
    }

    public boolean isUnknown() {
      return operand.isUnknown();
    }

    public List<EvaluatedInterval> evaluate() {
      List<EvaluatedInterval> result = new ArrayList();
      EvaluatedInterval previous = null;

      for (EvaluatedInterval i: operand.evaluate()) {
        if (previous == null) {
          if (i.low != Undefined) {
            result.add
              (new EvaluatedInterval
               (Undefined, BoundType.Inclusive,
                i.low, i.lowBoundType.opposite));
          }
        } else {
          result.add
            (new EvaluatedInterval
             (previous.high, previous.highBoundType.opposite,
              i.low, i.lowBoundType.opposite));
        }
        
        previous = i;
      }

      if (previous == null) {
        result.add(UnboundedEvaluatedInterval);
      } else if (previous.high != Undefined) {
        result.add
          (new EvaluatedInterval
           (previous.high, previous.highBoundType.opposite,
            Undefined, BoundType.Inclusive));
      }

      return result;
    }
  }

  private static interface LiveExpression extends Expression {
    public void visit(ExpressionVisitor visitor);
    public Object evaluate(boolean convertDummyToNull);
    public Scan makeScan(LiveColumnReference reference);
    public Class type();
  }

  private static class ExpressionContext {
    public final Map<MyExpression, LiveExpression> map = new HashMap();
    public final Set<LiveColumnReference> columnReferences = new HashSet();
    public final Object[] parameters;
    public int parameterIndex;

    public ExpressionContext(Object[] parameters) {
      this.parameters = parameters;
    }
  }

  private static interface MyExpression extends Expression {
    public void visit(ExpressionVisitor visitor);
    public LiveExpression makeLiveExpression(ExpressionContext context);
  }

  private static interface SourceIterator {
    public ResultType nextRow();
    public boolean rowUpdated();
  }

  private static interface MySource extends Source {
    public SourceIterator iterator(MyRevision base,
                                   NodeStack baseStack,
                                   MyRevision fork,
                                   NodeStack forkStack,
                                   LiveExpression test,
                                   ExpressionContext expressionContext,
                                   boolean visitUnchanged);
    public void visit(SourceVisitor visitor);
    public void visit(ExpressionContext expressionContext,
                      ColumnReferenceVisitor visitor);
  }

  private static class DiffPair {
    public Node base;
    public Node fork;
  }
  
  private static int compareForDescent(Node n,
                                       Comparable key,
                                       BoundType boundType,
                                       boolean high)
  {
    if (n == NullNode) {
      return 0;
    }

    int difference = compare(n.key, key, boundType, high);
    if (difference > 0) {
      n = n.left;
      if (n == NullNode) {
        return 0;
      } else if (compare(n.key, key, boundType, high) >= 0) {
        return 1;
      } else {
        while (n != NullNode && compare(n.key, key, boundType, high) < 0) {
          n = n.right;
        }

        if (n == NullNode) {
          // todo: cache this result so we don't have to loop every time
          return 0;
        } else {
          return 1;
        }
      }
    } else if (difference < 0) {
      n = n.right;
      if (n == NullNode) {
        return 0;
      } else {
        return -1;
      }
    } else {
      return 0;
    }
  }

  private static void descend(NodeStack stack, int oppositeDirection) {
    if (oppositeDirection > 0) {
      push(stack, stack.top.left);
    } else {
      push(stack, stack.top.right);
    }
  }

  private static void leftmost(NodeStack stack) {
    while (stack.top.left != NullNode) {
      push(stack, stack.top.left);
    }
  }

  private static void next(NodeStack stack) {
    if (stack.top != null) {
      if (stack.top.right != NullNode) {
        push(stack, stack.top.right);

        while (stack.top.left != NullNode) {
          push(stack, stack.top.left);
        }
      } else {
        ascendNext(stack);
      }
    }
  }

  private static void ascendNext(NodeStack stack) {
    while (stack.index != stack.base && peek(stack).right == stack.top) {
      pop(stack);
    }

    if (stack.index == stack.base) {
      clear(stack);
    } else {
      pop(stack);
    }
  }

  private static class DiffIterator {
    public final Node baseRoot;
    public final NodeStack base;
    public final Node forkRoot;
    public final NodeStack fork;
    public final Iterator<EvaluatedInterval> intervalIterator;
    public final boolean visitUnchanged;
    public EvaluatedInterval currentInterval;
    public boolean foundStart;

    public DiffIterator(Node baseRoot,
                        NodeStack base,
                        Node forkRoot,
                        NodeStack fork,
                        Iterator<EvaluatedInterval> intervalIterator,
                        boolean visitUnchanged)
    {
      this.baseRoot = baseRoot;
      this.base = base;
      this.forkRoot = forkRoot;
      this.fork = fork;
      this.intervalIterator = intervalIterator;
      this.visitUnchanged = visitUnchanged;
      this.currentInterval = intervalIterator.next();
    }

    public boolean next(DiffPair pair) {
      if (currentInterval != null) {
        if (next(currentInterval, pair)) {
          return true;
        }
      }

      while (true) {
        if (intervalIterator.hasNext()) {
          foundStart = false;
          clear(base);
          clear(fork);
          currentInterval = intervalIterator.next();
          if (next(currentInterval, pair)) {
            return true;
          }
        } else {
          return false;
        }
      }
    }

    private void findStart(EvaluatedInterval interval) {
      push(base, baseRoot);
      push(fork, forkRoot);

      while (true) {
        int baseDifference = compareForDescent
          (base.top, interval.low, interval.lowBoundType, false);

        int forkDifference = compareForDescent
          (fork.top, interval.low, interval.lowBoundType, false);

        if (baseDifference == 0) {
          if (forkDifference == 0) {
            break;
          } else {
            descend(fork, forkDifference);
          }
        } else if (forkDifference == 0) {
          descend(base, baseDifference);
        } else {
          int difference;
          if (base.top == fork.top) {
            if (visitUnchanged) {
              difference = 0;
            } else {
              if (baseDifference < 0) {
                clear(base);
                clear(fork);
              }
              break;
            }
          } else {
            difference = compare(base.top.key, fork.top.key);
          }

          if (difference > 0) {
            if (baseDifference > 0) {
              descend(base, baseDifference);
            } else {
              descend(fork, forkDifference);              
            }
          } else if (difference < 0) {
            if (forkDifference > 0) {
              descend(fork, forkDifference);
            } else {
              descend(base, baseDifference);              
            }
          } else {
            descend(base, baseDifference);
            descend(fork, forkDifference);            
          }
        }
      }
      
      if (base.top != null
          && (base.top == NullNode || compare
              (base.top.key, interval.low, interval.lowBoundType, false) < 0))
      {
        clear(base);
      }

      if (VerboseDiff) {
        System.out.println("base start from "
                           + (base.top == null ? "nothing" : base.top.key));
        dump(baseRoot, System.out, 0);
      }

      if (fork.top != null
          && (fork.top == NullNode || compare
              (fork.top.key, interval.low, interval.lowBoundType, false) < 0))
      {
        clear(fork);
      }

      if (VerboseDiff) {
        System.out.println("fork start from "
                           + (fork.top == null ? "nothing" : fork.top.key));
        dump(forkRoot, System.out, 0);
      }
    }

    private boolean next(EvaluatedInterval interval, DiffPair pair) {
      if (! foundStart) {
        findStart(interval);
        
        foundStart = true;
      }

      while (true) {
        int baseDifference = base.top == null ? 1 : compare
          (base.top.key, interval.high, interval.highBoundType, true);

        int forkDifference = fork.top == null ? 1 : compare
          (fork.top.key, interval.high, interval.highBoundType, true);
      
        if (baseDifference <= 0) {
          if (forkDifference <= 0) {
            int difference;
            if (base.top == fork.top) {
              if (visitUnchanged) {
                difference = 0;
              } else {
                ascendNext(base);
                ascendNext(fork);
                continue;
              }
            } else {
              difference = compare(base.top.key, fork.top.key);
            }

            if (difference > 0) {
              pair.base = null;
              pair.fork = fork.top;
              MyDBMS.next(fork); 
              return true;           
            } else if (difference < 0) {
              pair.base = base.top;
              pair.fork = null;
              MyDBMS.next(base);
              return true;
            } else {
              pair.base = base.top;
              pair.fork = fork.top;
              MyDBMS.next(base);
              MyDBMS.next(fork);
              return true;
            }
          } else {
            pair.base = base.top;
            pair.fork = null;
            MyDBMS.next(base);
            return true;
          }
        } else {
          pair.base = null;
          if (forkDifference <= 0) {
            pair.fork = fork.top;
            MyDBMS.next(fork);
            return true;
          } else {
            pair.fork = null;
            return false;
          }
        }
      }
    }
  }

  private static int compareForDescent(Node a, Node b) {
    if (a == null || b == null) {
      return 0;
    }

    int difference = compare(a.key, b.key);
    if (difference > 0) {
      if (a.left == NullNode) {
        return 0;
      } else {
        return 1;
      }
    } else if (difference < 0) {
      if (b.left == NullNode) {
        return 0;
      } else {
        return -1;
      }
    } else {
      return 0;
    }
  }

  private static void descendLeft(NodeStack s) {
    if (s.top != null && s.top.left != NullNode) {
      push(s, s.top.left);
    }
  }

  private static int compareForMerge(Node a, Node b) {
    if (a == null || b == null) {
      return 0;
    }

    return compare(a.key, b.key);
  }

  private static class MergeTriple {
    public Node base;
    public Node left;
    public Node right;
  }

  private static class MergeIterator {
    public final NodeStack base;
    public final NodeStack left;
    public final NodeStack right;

    public MergeIterator(Node baseRoot,
                         NodeStack base,
                         Node leftRoot,
                         NodeStack left,
                         Node rightRoot,
                         NodeStack right)
    {
      this.base = base;
      this.left = left;
      this.right = right;
      
      if (leftRoot == NullNode) {
        throw new NullPointerException();
      }

      if (rightRoot == NullNode) {
        throw new NullPointerException();
      }

      if (baseRoot != NullNode) {
        push(base, baseRoot);
      }
      push(left, leftRoot);
      push(right, rightRoot);

      expect(base.top != NullNode
             && left.top != NullNode
             && right.top != NullNode);

      // find leftmost nodes to start iteration
      while (true) {
        int leftBase = compareForDescent(left.top, base.top);
        if (leftBase > 0) {
          int rightBase = compareForDescent(right.top, base.top);
          if (rightBase > 0) {
            int leftRight = compareForDescent(left.top, right.top);
            if (leftRight > 0) {
              // base < right < left
              descendLeft(left);
            } else if (leftRight < 0) {
              // base < left < right
              descendLeft(right);
            } else {
              // base < left = right
              if (left.top.left == NullNode && right.top.left == NullNode) {
                while (base.top.left != NullNode) {
                  push(base, base.top.left);
                }
                break;
              } else {
                descendLeft(left);
                descendLeft(right);
              }
            }
          } else {
            // right <= base < left
            descendLeft(left);
          }
        } else if (leftBase < 0) {
          int rightBase = compareForDescent(right.top, base.top);
          if (rightBase > 0) {
            // left < base < right
            descendLeft(right);
          } else if (rightBase < 0) {
            // left/right < base
            descendLeft(base);
          } else {
            // left < right = base

            // todo: if right.top == base.top and we're basing the
            // merge result on left, we can break here instead of
            // descending further

            if (right.top.left == NullNode && base.top.left == NullNode) {
              while (left.top.left != NullNode) {
                push(left, left.top.left);
              }
              break;
            } else {
              descendLeft(right);
              descendLeft(base);
            }
          }
        } else {
          int rightBase = compareForDescent(right.top, base.top);
          if (rightBase > 0) {
            // left = base < right
            descendLeft(right);
          } else if (rightBase < 0) {
            // right < left = base
            if (left.top.left == NullNode && base.top.left == NullNode) {
              while (right.top.left != NullNode) {
                push(right, right.top.left);
              }
              break;
            } else {
              descendLeft(left);
              descendLeft(right);
            }
          } else {
            // right = left = base
            if (left.top == right.top && left.top == base.top) {
              // no need to go any deeper -- there aren't any changes
              break;
            } else if (left.top.left == NullNode
                       && right.top.left == NullNode
                       && (base.top == null || base.top.left == NullNode))
            {
              break;
            } else {
              descendLeft(left);
              descendLeft(right);
              descendLeft(base);
            }
          }
        }
      }

      expect(base.top != NullNode
             && left.top != NullNode
             && right.top != NullNode);
    }

    public boolean next(MergeTriple triple) {
      while (true) {
        int leftBase = compareForMerge(left.top, base.top);
        if (leftBase > 0) {
          int rightBase = compareForMerge(right.top, base.top);
          if (rightBase > 0) {
            // base < right/left
            triple.left = null;
            triple.right = null;
            triple.base = base.top;
            MyDBMS.next(base);
          } else if (rightBase < 0) {
            // right < base < left
            triple.left = null;
            triple.right = right.top;
            triple.base = null;
            MyDBMS.next(right);
          } else {
            // right = base < left
            if (right.top == null && base.top == null) {
              triple.left = left.top;
              triple.right = null;
              triple.base = null;
              MyDBMS.next(left);
            } else {
              // todo: if right.top == base.top and we're basing the
              // merge result on left, we can skip this part of the
              // tree

              triple.left = null;
              triple.right = right.top;
              triple.base = base.top;
              MyDBMS.next(right);
              MyDBMS.next(base);
            }
          }
        } else if (leftBase < 0) {
          int rightBase = compareForMerge(right.top, base.top);
          if (rightBase >= 0) {
            // left < right <= base
            triple.left = left.top;
            triple.right = null;
            triple.base = null;
            MyDBMS.next(left);
          } else {
            int leftRight = compareForMerge(left.top, right.top);
            if (leftRight > 0) {
              // right < left < base
              triple.left = null;
              triple.right = right.top;
              triple.base = null;
              MyDBMS.next(right);
            } else if (leftRight < 0) {
              // left < right < base
              triple.left = left.top;
              triple.right = null;
              triple.base = null;
              MyDBMS.next(left);
            } else {
              // left = right < base
              if (left.top == null && right.top == null) {
                triple.left = null;
                triple.right = null;
                triple.base = base.top;
                MyDBMS.next(base);
              } else {
                triple.left = left.top;
                triple.right = right.top;
                triple.base = null;
                MyDBMS.next(left);
                MyDBMS.next(right);
              }
            }
          }
        } else {
          int rightBase = compareForMerge(right.top, base.top);
          if (rightBase > 0) {
            // left = base < right
            if (left.top == null && base.top == null) {
              triple.left = null;
              triple.right = right.top;
              triple.base = null;
              MyDBMS.next(base);
            } else {
              triple.left = left.top;
              triple.right = null;
              triple.base = base.top;
              MyDBMS.next(left);
              MyDBMS.next(base);
            }
          } else if (rightBase < 0) {
            // right < left = base
            triple.left = null;
            triple.right = right.top;
            triple.base = null;
            MyDBMS.next(right);
          } else if (base.top == null) {
            int leftRight = compareForMerge(left.top, right.top);
            if (leftRight > 0) {
              // right < left
              triple.left = null;
              triple.right = right.top;
              triple.base = null;
              MyDBMS.next(right);
            } else if (leftRight < 0) {
              // left < right
              triple.left = left.top;
              triple.right = null;
              triple.base = null;
              MyDBMS.next(left);
            } else {
              // left = right
              if (left.top == null && right.top == null) {
                return false;
              } else {
                triple.left = left.top;
                triple.right = right.top;
                triple.base = null;
                MyDBMS.next(left);
                MyDBMS.next(right);
              }
            }
          } else {
            // left = right = base
            if (left.top == right.top && left.top == base.top) {
              // no need to go any deeper -- there aren't any changes
              ascendNext(left);
              ascendNext(right);
              ascendNext(base);
              continue;
            } else {
              triple.left = left.top;
              triple.right = right.top;
              triple.base = base.top;
              MyDBMS.next(left);
              MyDBMS.next(right);
              MyDBMS.next(base);
            }
          }
        }
        return true;
      }
    }
  }

  private static class Plan {
    public final MyIndex index;
    public final int size;
    public final LiveColumnReference[] references;
    public final Scan[] scans;
    public final DiffIterator[] iterators;
    public boolean match;
    public boolean complete = true;

    public Plan(MyIndex index) {
      this.index = index;
      this.size = index.columns.size();
      this.references = new LiveColumnReference[size];
      this.scans = new Scan[size];
      this.iterators = new DiffIterator[size];
    }
  }

  private static Plan improvePlan(Plan best,
                                  MyIndex index,
                                  LiveExpression test,
                                  MyTableReference tableReference)
  {
    Plan plan = new Plan(index);

    for (int i = 0; i < plan.size; ++i) {
      MyColumn column = index.columns.get(i);

      LiveColumnReference reference = findColumnReference
        (test, tableReference, column);

      boolean match = false;

      if (reference != null) {
        Scan scan = test.makeScan(reference);

        plan.scans[i] = scan;

        if (scan.isUseful()) {
          plan.match = true;
          match = true;
        }

        reference.value = Dummy;
        plan.references[i] = reference;
      } else {
        plan.scans[i] = UnboundedInterval;              
      }

      if (! match) {
        plan.complete = false;
      }
    }

    for (int i = 0; i < plan.size; ++i) {
      LiveColumnReference reference = plan.references[i];
      if (reference != null) {
        reference.value = Undefined;
      }
    }
            
    if (best == null
        || (plan.match && (! best.match))
        || (plan.complete && (! best.complete)))
    {
      best = plan;
    }
    
    return best;
  }

  private static Node pathFind(Node root, Comparable ... path) {
    for (Comparable c: path) {
      root = (Node) find(root, c).value;
    }
    return root;
  }

  private static Plan choosePlan(MyRevision base,
                                 NodeStack baseStack,
                                 MyRevision fork,
                                 NodeStack forkStack,
                                 LiveExpression test,
                                 MyTableReference tableReference)
  {
    Plan best = improvePlan
      (null, tableReference.table.primaryKey, test, tableReference);

    DiffIterator indexIterator = new DiffIterator
      (pathFind(base.root, IndexTable, IndexTable.primaryKey.key,
                tableReference.table),
       baseStack = new NodeStack(baseStack),
       pathFind(fork.root, IndexTable, IndexTable.primaryKey.key,
                tableReference.table),
       forkStack = new NodeStack(forkStack),
       list(UnboundedEvaluatedInterval).iterator(),
       true);

    boolean baseEmpty = find(base.root, tableReference.table) == NullNode;

    boolean forkEmpty = find(fork.root, tableReference.table) == NullNode;

    DiffPair pair = new DiffPair();
    while (indexIterator.next(pair)) {
      if ((pair.base == null && ! baseEmpty)
          || (pair.fork == null && ! forkEmpty))
      {
        continue;
      }

      IndexKey indexKey = (IndexKey)
        (pair.base == null ? pair.fork.key : pair.base.key);

      if (! indexKey.equals(tableReference.table.primaryKey.key)) {
        best = improvePlan(best, indexKey.index, test, tableReference);
      }
    }

    popStack(baseStack);
    popStack(forkStack);

    return best;
  }

  private static boolean valuesEqual(Node a, Node b) {
    return (a != null && b != null && equal(a.value, b.value));
  }

  private static boolean treeEqual(NodeStack baseStack,
                                   Node base,
                                   NodeStack forkStack,
                                   Node fork)
  {
    if (base == NullNode) {
      return fork == NullNode;
    } else if (fork == NullNode) {
      return base == NullNode;
    } else {
      DiffIterator indexIterator = new DiffIterator
        (base, baseStack = new NodeStack(baseStack),
         fork, forkStack = new NodeStack(forkStack),
         list(UnboundedEvaluatedInterval).iterator(),
         true);

      DiffPair pair = new DiffPair();
      boolean result = true;
      while (indexIterator.next(pair)) {
        if (! valuesEqual(pair.base, pair.fork)) {
          result = false;
          break;
        }
      }

      popStack(baseStack);
      popStack(forkStack);

      return result;
    }
  }

  private static Object value(Node n) {
    return (n == NullNode ? null : n.value);
  }

  private static class MyTableReference implements TableReference, MySource {
    public class MySourceIterator implements SourceIterator {
      public final Node base;
      public final Node fork;
      public final LiveExpression test;
      public final ExpressionContext expressionContext;
      public final boolean visitUnchanged;
      public final List<LiveColumnReference> columnReferences
        = new ArrayList();
      public final Plan plan;
      public final DiffPair pair = new DiffPair();
      public NodeStack baseStack;
      public NodeStack forkStack;
      public int depth;
      public boolean testFork;

      public MySourceIterator(MyRevision base,
                              NodeStack baseStack,
                              MyRevision fork,
                              NodeStack forkStack,
                              LiveExpression test,
                              ExpressionContext expressionContext,
                              Plan plan,
                              boolean visitUnchanged)
      {
        this.base = pathFind(base.root, table);
        this.fork = pathFind(fork.root, table);
        this.test = test;
        this.expressionContext = expressionContext;
        this.visitUnchanged = visitUnchanged;
        this.plan = plan;

        plan.iterators[0] = new DiffIterator
          (pathFind(this.base, plan.index.key),
           this.baseStack = new NodeStack(baseStack),
           pathFind(this.fork, plan.index.key),
           this.forkStack = new NodeStack(forkStack),
           plan.scans[0].evaluate().iterator(),
           visitUnchanged);

        for (LiveColumnReference r: expressionContext.columnReferences) {
          if (r.tableReference == MyTableReference.this) {
            // skip references which will be populated as part of the
            // index scan:
            for (int j = 0; j < plan.size - 1; ++j) {
              if (r == plan.references[j]) {
                r = null;
                break;
              }
            }

            if (r != null) {
              columnReferences.add(r);
            }
          }
        }
      }

      public ResultType nextRow() {
        if (testFork) {
          testFork = false;
          if (test(pair.fork)) {
            return ResultType.Inserted;
          }
        }

        while (true) {
          if (plan.iterators[depth].next(pair)) {
            if (depth == plan.size - 1) {
              if (test(pair.base)) {
                if (pair.fork == null) {
                  return ResultType.Deleted;
                } else if (pair.base == pair.fork
                           || treeEqual(baseStack, (Node) pair.base.value,
                                        forkStack, (Node) pair.fork.value))
                {
                  if (visitUnchanged) {
                    return ResultType.Unchanged;
                  }
                } else {
                  testFork = true;
                  return ResultType.Deleted;                  
                }
              } else if (test(pair.fork)) {
                return ResultType.Inserted;
              }
            } else {
              descend(pair);
            }
          } else if (depth == 0) {
            // todo: be defensive to ensure we can safely keep
            // returning ResultType.End if the application calls
            // nextRow again after this.  The popStack calls below
            // should not be called more than once.

            for (LiveColumnReference r: columnReferences) {
              r.value = Undefined;
            }

            popStack(baseStack);
            popStack(forkStack);

            return ResultType.End;
          } else {
            ascend();
          }
        }
      }

      public boolean rowUpdated() {
        return depth == plan.size - 1
          && pair.base != null
          && pair.fork != null
          && testFork
          && test(pair.fork);
      }

      private boolean test(Node node) {
        if (node != null) {
          Node tree = (Node) node.value;
        
          for (LiveColumnReference r: columnReferences) {
            r.value = value(find(tree, r.column));
          }

          Object result = test.evaluate(false);

          if (VerboseTest) {
            for (LiveColumnReference r: expressionContext.columnReferences) {
              System.out.print(r.value + " ");
            }
            System.out.println(": " + result);
          }

          return result != Boolean.FALSE;
        } else {
          return false;
        }
      }

      private void descend(DiffPair pair) {
        Node base = pair.base;
        Node fork = pair.fork;
        
        LiveColumnReference reference = plan.references[depth];
        if (reference != null) {
          reference.value = base == null ? fork.key : base.key;
        }

        ++ depth;

        plan.iterators[depth] = new DiffIterator
          (base == null ? NullNode : (Node) base.value,
           baseStack = new NodeStack(baseStack),
           fork == null ? NullNode : (Node) fork.value,
           forkStack = new NodeStack(forkStack),
           plan.scans[depth].evaluate().iterator(),
           visitUnchanged);
      }

      private void ascend() {
        plan.iterators[depth] = null;

        -- depth;

        LiveColumnReference reference = plan.references[depth];
        if (reference != null) {
          reference.value = Undefined;
        }

        baseStack = popStack(baseStack);
        forkStack = popStack(forkStack);
      }
    }

    public final MyTable table;

    public MyTableReference(MyTable table) {
      this.table = table;
    }

    public MySourceIterator iterator(MyRevision base,
                                     NodeStack baseStack,
                                     MyRevision fork,
                                     NodeStack forkStack,
                                     LiveExpression test,
                                     ExpressionContext expressionContext,
                                     Plan plan,
                                     boolean visitUnchanged)
    {
      return new MySourceIterator
        (base, baseStack, fork, forkStack, test, expressionContext, plan,
         visitUnchanged);
    }

    public MySourceIterator iterator(MyRevision base,
                                     NodeStack baseStack,
                                     MyRevision fork,
                                     NodeStack forkStack,
                                     LiveExpression test,
                                     ExpressionContext expressionContext,
                                     boolean visitUnchanged)
    {
      return iterator
        (base, baseStack, fork, forkStack, test, expressionContext, choosePlan
         (base, baseStack, fork, forkStack, test, MyTableReference.this),
         visitUnchanged);
    }

    public void visit(SourceVisitor visitor) {
      visitor.visit(this);
    }

    public void visit(ExpressionContext expressionContext,
                      ColumnReferenceVisitor visitor)
    {
      for (LiveColumnReference r: expressionContext.columnReferences) {
        if (r.tableReference.table == table) {
          visitor.visit(r);
        }
      }
    }
  }

  private static LiveColumnReference findColumnReference
    (ExpressionContext context,
     MyTableReference tableReference,
     MyColumn column)
  {
    for (LiveColumnReference r: context.columnReferences) {
      if (r.tableReference == tableReference
          && r.column == column)
      {
        return r;
      }
    }
    return null;
  }

  private static LiveColumnReference findColumnReference
    (LiveExpression expression,
     MyTableReference tableReference,
     MyColumn column)
  {
    ColumnReferenceFinder finder = new ColumnReferenceFinder
      (tableReference, column);
    expression.visit(finder);
    return finder.reference;
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

    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof MyColumnReference) {
        MyColumnReference r = (MyColumnReference) o;
        return r.tableReference.equals(tableReference)
          && r.column.equals(column);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return tableReference.hashCode() ^ column.hashCode();
    }

    public void visit(ExpressionVisitor visitor) {
      visitor.visit(this);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      LiveExpression e = context.map.get(this);
      if (e == null) {
        LiveColumnReference r = new LiveColumnReference
          (tableReference, column);
        context.map.put(this, r);
        context.columnReferences.add(r);
        return r;
      } else {
        return e;
      }
    }
  }

  private static class LiveColumnReference implements LiveExpression {
    public final MyTableReference tableReference;
    public final MyColumn column;
    public Object value = Undefined;

    public LiveColumnReference(MyTableReference tableReference,
                               MyColumn column)
    {
      this.tableReference = tableReference;
      this.column = column;
    }

    public void visit(ExpressionVisitor visitor) {
      visitor.visit(this);
    }

    public Object evaluate(boolean convertDummyToNull) {
      return convertDummyToNull && value == Dummy ? null : value ;
    }

    public Scan makeScan(LiveColumnReference reference) {
      throw new UnsupportedOperationException();
    }

    public Class type() {
      return column.type;
    }
  }

  private interface ColumnReferenceVisitor {
    public void visit(LiveColumnReference r);
  }

  private static class Constant implements MyExpression, LiveExpression {
    public final Object value;
    
    public Constant(Object value) {
      this.value = value;
    }

    public Object evaluate(boolean convertDummyToNull) {
      return value;
    }

    public Scan makeScan(LiveColumnReference reference) {
      throw new UnsupportedOperationException();
    }

    public void visit(ExpressionVisitor visitor) {
      visitor.visit(this);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      return this;
    }

    public Class type() {
      return value == null ? null : value.getClass();
    }
  }

  private static class Parameter implements MyExpression {
    public void visit(ExpressionVisitor visitor) {
      visitor.visit(this);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      LiveExpression e = context.map.get(this);
      if (e == null) {
        context.map.put
          (this, 
           e = new Constant(context.parameters[context.parameterIndex++]));
      }

      return e;
    }
  }

  private static class Comparison implements MyExpression {
    public final BinaryOperationType type;
    public final MyExpression left;
    public final MyExpression right;
    
    public Comparison(BinaryOperationType type,
                      MyExpression left,
                      MyExpression right)
    {
      this.type = type;
      this.left = left;
      this.right = right;
    }

    public void visit(ExpressionVisitor visitor) {
      left.visit(visitor);
      right.visit(visitor);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      return new LiveComparison
        (type,
         left.makeLiveExpression(context),
         right.makeLiveExpression(context));
    }
  }

  private static boolean comparable(Class a, Class b) {
    return a == null || b == null
      || (Comparable.class.isAssignableFrom(a)
          && Comparable.class.isAssignableFrom(b)
          && (a.isAssignableFrom(b) || b.isAssignableFrom(a)));
  }

  private static class LiveComparison implements LiveExpression {
    public final BinaryOperationType type;
    public final LiveExpression left;
    public final LiveExpression right;
    
    public LiveComparison(BinaryOperationType type,
                          LiveExpression left,
                          LiveExpression right)
    {
      this.type = type;
      this.left = left;
      this.right = right;

      if (! comparable(left.type(), right.type())) {
        throw new ClassCastException
          ("types not comparable: " + left.type() + " and " + right.type());
      }
    }

    public void visit(ExpressionVisitor visitor) {
      left.visit(visitor);
      right.visit(visitor);
    }

    public Object evaluate(boolean convertDummyToNull) {
      Object leftValue = left.evaluate(convertDummyToNull);
      Object rightValue = right.evaluate(convertDummyToNull);

      if (leftValue == null || rightValue == null) {
        return false;
      } else if (leftValue == Undefined || rightValue == Undefined) {
        return Undefined;
      } else {
        switch (type) {
        case Equal:
          return leftValue.equals(rightValue);

        case NotEqual:
          return ! leftValue.equals(rightValue);

        case GreaterThan:
          return ((Comparable) leftValue).compareTo(rightValue) > 0;

        case GreaterThanOrEqual:
          return ((Comparable) leftValue).compareTo(rightValue) >= 0;

        case LessThan:
          return ((Comparable) leftValue).compareTo(rightValue) < 0;

        case LessThanOrEqual:
          return ((Comparable) leftValue).compareTo(rightValue) <= 0;

        default: throw new RuntimeException
            ("unexpected comparison type: " + type);
        }
      }
    }

    public Scan makeScan(LiveColumnReference reference) {
      if (left == reference) {
        if (right.evaluate(false) == Undefined) {
          return UnknownInterval;
        } else {
          switch (type) {
          case Equal:
            return new Interval(right, right);

          case NotEqual:
            return UnboundedInterval;

          case GreaterThan:
            return new Interval(right, BoundType.Exclusive,
                                UndefinedExpression, BoundType.Inclusive);

          case GreaterThanOrEqual:
            return new Interval(right, UndefinedExpression);

          case LessThan:
            return new Interval(UndefinedExpression, BoundType.Inclusive,
                                right, BoundType.Exclusive);

          case LessThanOrEqual:
            return new Interval(UndefinedExpression, right);

          default: throw new RuntimeException
              ("unexpected comparison type: " + type);
          }
        }
      } else if (right == reference) {
        if (left.evaluate(false) == Undefined) {
          return UnknownInterval;
        } else {
          switch (type) {
          case Equal:
            return new Interval(left, left);

          case NotEqual:
            return UnboundedInterval;

          case GreaterThan:
            return new Interval(UndefinedExpression, BoundType.Inclusive,
                                left, BoundType.Exclusive);

          case GreaterThanOrEqual:
            return new Interval(UndefinedExpression, left);

          case LessThan:
            return new Interval(left, BoundType.Exclusive,
                                UndefinedExpression, BoundType.Inclusive);

          case LessThanOrEqual:
            return new Interval(left, UndefinedExpression);

          default: throw new RuntimeException
              ("unexpected comparison type: " + type);
          }
        }
      } else {
        return UnboundedInterval;
      }
    }

    public Class type() {
      return Boolean.class;
    }
  }

  private static class BooleanBinaryOperation implements MyExpression {
    public final BinaryOperationType type;
    public final MyExpression left;
    public final MyExpression right;
    
    public BooleanBinaryOperation(BinaryOperationType type,
                                  MyExpression left,
                                  MyExpression right)
    {
      this.type = type;
      this.left = left;
      this.right = right;
    }

    public void visit(ExpressionVisitor visitor) {
      left.visit(visitor);
      right.visit(visitor);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      return new LiveBooleanBinaryOperation
        (type,
         left.makeLiveExpression(context),
         right.makeLiveExpression(context));
    }
  }

  private static class LiveBooleanBinaryOperation implements LiveExpression {
    public final BinaryOperationType type;
    public final LiveExpression left;
    public final LiveExpression right;
    
    public LiveBooleanBinaryOperation(BinaryOperationType type,
                                      LiveExpression left,
                                      LiveExpression right)
    {
      this.type = type;
      this.left = left;
      this.right = right;

      if (left.type() != Boolean.class) {
        throw new ClassCastException
          (left.type() + " cannot be cast to " + Boolean.class);
      } else if (right.type() != Boolean.class) {
        throw new ClassCastException
          (right.type() + " cannot be cast to " + Boolean.class);
      }
    }

    public void visit(ExpressionVisitor visitor) {
      left.visit(visitor);
      right.visit(visitor);
    }

    public Object evaluate(boolean convertDummyToNull) {
      Object leftValue = left.evaluate(convertDummyToNull);
      Object rightValue = right.evaluate(convertDummyToNull);

      if (leftValue == null || rightValue == null) {
        return false;
      } else if (leftValue == Undefined || rightValue == Undefined) {
        return Undefined;
      } else {
        switch (type) {
        case And:
          return leftValue == Boolean.TRUE && rightValue == Boolean.TRUE;

        case Or:
          return leftValue == Boolean.TRUE || rightValue == Boolean.TRUE;

        default: throw new RuntimeException
            ("unexpected comparison type: " + type);
        }
      }
    }

    public Scan makeScan(LiveColumnReference reference) {
      Scan leftScan = left.makeScan(reference);
      Scan rightScan = right.makeScan(reference);

      switch (type) {
      case And:
        return new Intersection(leftScan, rightScan);

      case Or:
        return new Union(leftScan, rightScan);

      default: throw new RuntimeException
          ("unexpected comparison type: " + type);
      }
    }

    public Class type() {
      return Boolean.class;
    }
  }

  private static class BooleanUnaryOperation implements MyExpression {
    public final UnaryOperationType type;
    public final MyExpression operand;
    
    public BooleanUnaryOperation(UnaryOperationType type,
                                 MyExpression operand)
    {
      this.type = type;
      this.operand = operand;
    }

    public void visit(ExpressionVisitor visitor) {
      operand.visit(visitor);
    }

    public LiveExpression makeLiveExpression(ExpressionContext context) {
      return new LiveBooleanUnaryOperation
        (type, operand.makeLiveExpression(context));
    }
  }

  private static class LiveBooleanUnaryOperation implements LiveExpression {
    public final UnaryOperationType type;
    public final LiveExpression operand;
    
    public LiveBooleanUnaryOperation(UnaryOperationType type,
                                     LiveExpression operand)
    {
      this.type = type;
      this.operand = operand;
    }

    public void visit(ExpressionVisitor visitor) {
      operand.visit(visitor);
    }

    public Object evaluate(boolean convertDummyToNull) {
      Object value = operand.evaluate(convertDummyToNull);

      if (value == null) {
        return false;
      } else if (value == Undefined) {
        return Undefined;
      } else {
        switch (type) {
        case Not:
          return value != Boolean.TRUE;

        default: throw new RuntimeException
            ("unexpected comparison type: " + type);
        }
      }
    }

    public Scan makeScan(LiveColumnReference reference) {
      Scan scan = operand.makeScan(reference);

      switch (type) {
      case Not:
        return new Negation(scan);

      default: throw new RuntimeException
          ("unexpected comparison type: " + type);
      }
    }

    public Class type() {
      return Boolean.class;
    }
  }

  private static class Join implements MySource {
    public class MySourceIterator implements SourceIterator {
      public final MyRevision base;
      public final MyRevision fork;
      public final LiveExpression test;
      public final ExpressionContext expressionContext;
      public final boolean visitUnchanged;
      public final SourceIterator leftIterator;
      public NodeStack rightBaseStack;
      public NodeStack rightForkStack;
      public ResultType leftType;
      public SourceIterator rightIterator;
      public boolean sawRightUnchanged;
      public boolean sawRightInsert;
      public boolean sawRightDelete;
      public boolean sawRightEnd;
      public boolean setUndefinedReferences;

      public MySourceIterator(MyRevision base,
                              NodeStack baseStack,
                              MyRevision fork,
                              NodeStack forkStack,
                              LiveExpression test,
                              ExpressionContext expressionContext,
                              boolean visitUnchanged)
      {
        this.base = base;
        this.fork = fork;
        this.test = test;
        this.expressionContext = expressionContext;
        this.visitUnchanged = visitUnchanged;
        this.leftIterator = left.iterator
          (base, baseStack, fork, forkStack, test, expressionContext, true);
      }

      private void setUndefinedReferences() {
        setUndefinedReferences = true;

        right.visit
          (expressionContext, new ColumnReferenceVisitor() {
              public void visit(LiveColumnReference r) {
                r.value = Dummy;
              }
            });
      }

      public ResultType nextRow() {
        while (true) {
          if (sawRightEnd) {
            if (setUndefinedReferences) {
              setUndefinedReferences = false;
              right.visit
                (expressionContext, new ColumnReferenceVisitor() {
                    public void visit(LiveColumnReference r) {
                      r.value = Undefined;
                    }
                  });
            }
            sawRightUnchanged = false;
            sawRightInsert = false;
            sawRightDelete = false;
            sawRightEnd = false;
            rightIterator = null;
          }

          if (rightIterator == null) {
            leftType = leftIterator.nextRow();
            switch (leftType) {
            case End:
              return ResultType.End;

            case Unchanged:
              if (rightBaseStack == null) {
                rightBaseStack = new NodeStack();
              }

              if (rightForkStack == null) {
                rightForkStack = new NodeStack();
              }

              rightIterator = right.iterator
                (base, rightBaseStack, fork, rightForkStack, test,
                 expressionContext,
                 visitUnchanged || type == JoinType.LeftOuter);
              break;

            case Inserted:
              if (rightForkStack == null) {
                rightForkStack = new NodeStack();
              }

              rightIterator = right.iterator
                (EmptyRevision, NullNodeStack, fork, rightForkStack, test,
                 expressionContext, true);
              break;

            case Deleted:
              if (rightForkStack == null) {
                rightForkStack = new NodeStack();
              }

              rightIterator = right.iterator
                (EmptyRevision, NullNodeStack, base, rightForkStack, test,
                 expressionContext, true);
              break;

            default: throw new RuntimeException
                ("unexpected result type: " + leftType);
            }
          }

          ResultType rightType = rightIterator.nextRow();
          switch (rightType) {
          case End:
            sawRightEnd = true;
            if (type == JoinType.LeftOuter) {
              switch (leftType) {
              case Unchanged:
                if (sawRightInsert) {
                  if (! (sawRightDelete || sawRightUnchanged)) {
                    setUndefinedReferences();
                    return ResultType.Deleted;
                  }
                } else if (sawRightDelete && ! sawRightUnchanged) {
                  setUndefinedReferences();
                  return ResultType.Inserted;
                }
                break;

              case Inserted:
                if (! sawRightInsert) {
                  setUndefinedReferences();
                  return ResultType.Inserted;
                }
                break;
              
              case Deleted:
                if (! sawRightInsert) {
                  setUndefinedReferences();
                  return ResultType.Deleted;
                }
                break;

              default: throw new RuntimeException
                  ("unexpected result type: " + leftType);
              }
            }
            break;

          case Unchanged:
            sawRightUnchanged = true;
            expect(leftType == ResultType.Unchanged);
            if (visitUnchanged) {
              return ResultType.Unchanged;
            }
            break;

          case Inserted:
            sawRightInsert = true;
            switch (leftType) {
            case Unchanged:
            case Inserted:
              return ResultType.Inserted;

            case Deleted:
              return ResultType.Deleted;

            default: throw new RuntimeException
                ("unexpected result type: " + leftType);
            }

          case Deleted:
            sawRightDelete = true;
            expect(leftType == ResultType.Unchanged);
            return ResultType.Deleted;

          default: throw new RuntimeException
              ("unexpected result type: " + rightType);
          }
        }
      }

      public boolean rowUpdated() {
        return false;
      }
    }

    public final JoinType type;
    public final MySource left;
    public final MySource right;

    public Join(JoinType type,
                MySource left,
                MySource right)
    {
      this.type = type;
      this.left = left;
      this.right = right;
    }

    public SourceIterator iterator(MyRevision base,
                                   NodeStack baseStack,
                                   MyRevision fork,
                                   NodeStack forkStack,
                                   LiveExpression test,
                                   ExpressionContext expressionContext,
                                   boolean visitUnchanged)
    {
      return new MySourceIterator
        (base, baseStack, fork, forkStack, test, expressionContext,
         visitUnchanged);
    }

    public void visit(SourceVisitor visitor) {
      left.visit(visitor);
      right.visit(visitor);
    }

    public void visit(ExpressionContext expressionContext,
                      ColumnReferenceVisitor visitor) {
      left.visit(expressionContext, visitor);
      right.visit(expressionContext, visitor);
    }
  }

  private static class MyQueryTemplate implements QueryTemplate {
    public final int parameterCount;
    public final List<MyExpression> expressions;
    public final MySource source;
    public final MyExpression test;

    public MyQueryTemplate(int parameterCount,
                           List<MyExpression> expressions,
                           MySource source,
                           MyExpression test)
    {
      this.parameterCount = parameterCount;
      this.expressions = expressions;
      this.source = source;
      this.test = test;
    }
  }

  private interface SourceVisitor {
    public void visit(Source source);
  }

  private static class MyQueryResult implements QueryResult {
    private static class ChangeFinder implements SourceVisitor {
      public final MyRevision base;
      public final MyRevision fork;
      public boolean foundChanged;

      public ChangeFinder(MyRevision base, MyRevision fork) {
        this.base = base;
        this.fork = fork;
      }

      public void visit(Source source) {
        if (source instanceof MyTableReference) {
          MyTableReference reference = (MyTableReference) source;
          if (find(base.root, reference.table)
              != find(fork.root, reference.table))
          {
            foundChanged = true;
          }
        }
      }
    }

    public final List<LiveExpression> expressions;
    public final SourceIterator iterator;
    public int nextItemIndex;

    public MyQueryResult(MyRevision base,
                         MyRevision fork,
                         MyQueryTemplate template,
                         Object[] parameters)
    {
      if (base == fork) {
        expressions = null;
        iterator = null;
      } else {
        ChangeFinder finder = new ChangeFinder(base, fork);
        MySource source = template.source;
        source.visit(finder);

        if (finder.foundChanged) {
          expressions = new ArrayList(template.expressions.size());

          ExpressionContext context = new ExpressionContext(parameters);

          for (MyExpression e: template.expressions) {
            expressions.add(e.makeLiveExpression(context));
          }

          iterator = source.iterator
            (base, new NodeStack(), fork, new NodeStack(),
             template.test.makeLiveExpression(context), context, false);
        } else {
          expressions = null;
          iterator = null;
        }
      }
    }

    public ResultType nextRow() {
      if (iterator == null) {
        return ResultType.End;
      } else {
        nextItemIndex = 0;
        return iterator.nextRow();
      }
    }

    public Object nextItem() {
      if (iterator == null || nextItemIndex > expressions.size()) {
        throw new NoSuchElementException();
      } else {
        return expressions.get(nextItemIndex++).evaluate(true);
      }      
    }

    public boolean rowUpdated() {
      return iterator != null && iterator.rowUpdated();
    }
  }

  private static abstract class MyPatchTemplate implements PatchTemplate {
    public final int parameterCount;

    public MyPatchTemplate(int parameterCount) {
      this.parameterCount = parameterCount;
    }

    public abstract int apply(MyPatchContext context,
                              Object ... parameters);
  }

  private static class MyDiffResult implements DiffResult {
    public enum State {
      Start, Descend, Ascend, DescendValue, Value, Iterate, End;
    }

    public final DiffIterator[] iterators = new DiffIterator[MaxDepth];
    public final DiffPair pair = new DiffPair();
    public State state = State.Start;
    public NodeStack baseStack;
    public NodeStack forkStack;
    public MyTable table;
    public int depth;
    public int bottom;

    public MyDiffResult(MyRevision base,
                        NodeStack baseStack,
                        MyRevision fork,
                        NodeStack forkStack)
    {
      iterators[0] = new DiffIterator
        (base.root,
         this.baseStack = new NodeStack(baseStack),
         fork.root,
         this.forkStack = new NodeStack(forkStack),
         list(UnboundedEvaluatedInterval).iterator(),
         false);
    }

    public DiffResultType next() {
      while (true) {
        switch (state) {
        case Descend:
          descend();
          break;

        case Ascend:
          ascend();
          break;

        case DescendValue:
          state = State.Value;
          return DiffResultType.Value;

        case Start:
        case Value:
          state = State.Iterate;
          break;

        case Iterate:
          if (iterators[depth].next(pair)) {
            if (depth == TableDataDepth) {
              table = (MyTable) get();
              state = State.Descend;
              return DiffResultType.Key;
            } else if (depth == IndexDataDepth) {
              IndexKey indexKey = (IndexKey)
                (pair.base == null ? pair.fork.key : pair.base.key);

              if (equal(indexKey, table.primaryKey.key)) {
                bottom = indexKey.index.columns.size() + IndexDataBodyDepth;
                descend();
              }
            } else if (depth == bottom) {
              state = State.DescendValue;
              return DiffResultType.Key;
            } else {
              state = State.Descend;
              return DiffResultType.Key;
            }
          } else if (depth == 0) {
            state = State.End;
          } else {
            state = State.Ascend;
            return DiffResultType.Ascend;
          }
          break;

        case End:
          return DiffResultType.End;

        default:
          throw new RuntimeException("unexpected state: " + state);
        }
      }
    }

    private void descend() {
      Node base = pair.base;
      Node fork = pair.fork;

      ++ depth;

      iterators[depth] = new DiffIterator
        (base == null ? NullNode : (Node) base.value,
         baseStack = new NodeStack(baseStack),
         fork == null ? NullNode : (Node) fork.value,
         forkStack = new NodeStack(forkStack),
         list(UnboundedEvaluatedInterval).iterator(),
         false);
    }

    private void ascend() {
      iterators[depth] = null;

      -- depth;

      baseStack = popStack(baseStack);
      forkStack = popStack(forkStack);
    }

    // todo: use enum-value-specific virtual methods instead of switch
    // statements for the following four methods:

    public Object get() {
      switch (state) {
      case Value:
        return pair.base == null ? pair.fork.value : pair.base.value;

      case Start:
      case End:
        throw new IllegalStateException();

      case Descend:
      case Ascend:
      case DescendValue:
      case Iterate:
        return pair.base == null ? pair.fork.key : pair.base.key;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }

    public boolean baseHasKey() {
      switch (state) {
      case Value:
      case Start:
      case End:
        throw new IllegalStateException();

      case Descend:
      case Ascend:
      case DescendValue:
      case Iterate:
        return pair.base != null;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }

    public boolean forkHasKey() {
      switch (state) {
      case Value:
      case Start:
      case End:
        throw new IllegalStateException();

      case Descend:
      case Ascend:
      case DescendValue:
      case Iterate:
        return pair.fork != null;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }

    public void skip() {
      switch (state) {
      case Value:
      case Start:
      case End:
      case Ascend:
      case Iterate:
        throw new IllegalStateException();

      case Descend:
      case DescendValue:
        state = State.Iterate;
        break;

      default:
        throw new RuntimeException("unexpected state: " + state);
      }
    }
  }

  private static class MyPatchContext implements PatchContext {
    public Object token;
    public final NodeStack stack;
    public final Comparable[] keys;
    public final Node[] blazedRoots;
    public final Node[] blazedLeaves;
    public final Node[] found;
    public final BlazeResult blazeResult = new BlazeResult();
    public NodeStack indexUpdateBaseStack;
    public NodeStack indexUpdateForkStack;
    public MyRevision indexBase;
    public MyRevision result;
    public int max = -1;
    public boolean dirtyIndexes;

    public MyPatchContext(Object token,
                          MyRevision result,
                          NodeStack stack)
    {
      this.token = token;
      this.indexBase = result;
      this.result = result;
      this.stack = stack;
      keys = new Comparable[MaxDepth + 1];
      blazedRoots = new Node[MaxDepth + 1];
      blazedLeaves = new Node[MaxDepth + 1];
      found = new Node[MaxDepth + 1];
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
      if (max < index || ! equal(key, keys[index])) {
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
      result = EmptyRevision;
      max = -1;
    }

    private void delete(int index) {
      Node root = blazedRoots[index];
      if (root == null) {
        if (index == 0) {
          root = MyDBMS.delete(token, stack, result.root, keys[0]);

          if (root != result.root) {
            result = getRevision(token, result, root);
          }
        } else {
          Node original = find(index);
          Node originalRoot = (Node) find(index - 1).value;

          if (original == NullNode) {
            throw new RuntimeException();
          } else if (original == originalRoot
                     && original.left == NullNode
                     && original.right == NullNode)
          {
            delete(index - 1);
          } else {
            root = MyDBMS.delete
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
            n = MyDBMS.find(result.root, keys[0]);
            found[0] = n;
          } else {
            n = MyDBMS.find((Node) find(index - 1).value, keys[index]);
            found[index] = n;
          }
        }
      }
      return n;
    }

    private void deleteBlazed(int index) {
      blazedLeaves[index] = null;
      Node root = MyDBMS.delete(token, stack, blazedRoots[index], keys[index]);
      blazedRoots[index] = root;
      blazedLeaves[index] = null;
      if (root == null) {
        if (index == 0) {
          result.root = MyDBMS.delete(token, stack, result.root, keys[0]);
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
          Node root = MyDBMS.blaze
            (blazeResult, token, stack, result.root, keys[0]);

          if (root != result.root) {
            result = getRevision(token, result, root);
          }

          blazedRoots[0] = root;
          blazedLeaves[0] = blazeResult.node;
          blazedRoots[1] = (Node) blazeResult.node.value;
          return blazeResult.node;
        } else {
          Node root = MyDBMS.blaze
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
  }

  private static void updateIndexTree(MyPatchContext context,
                                      IndexKey indexKey,
                                      MyRevision base,
                                      NodeStack baseStack,
                                      NodeStack forkStack)
  {
    expect(indexKey != indexKey.index.table.primaryKey.key);

    List<MyColumn> keyColumns = indexKey.index.columns;

    MyTableReference.MySourceIterator iterator
      = new MyTableReference(indexKey.index.table).iterator
      (base, baseStack, context.result, forkStack, TrueConstant,
       new ExpressionContext(null), false);

    context.setKey(TableDataDepth, indexKey.index.table);
    context.setKey(IndexDataDepth, indexKey);

    boolean done = false;
    while (! done) {
      ResultType type = iterator.nextRow();
      switch (type) {
      case End:
        done = true;
        break;
      
      case Inserted: {
        Node tree = (Node) iterator.pair.fork.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          context.setKey
            (i + IndexDataBodyDepth,
             (Comparable) find(tree, keyColumns.get(i)).value);
        }

        Node n = context.blaze
          (i + IndexDataBodyDepth,
           (Comparable) find(tree, keyColumns.get(i)).value);

        expect(n.value == NullNode);
      
        n.value = tree;
      } break;

      case Deleted: {
        Node tree = (Node) iterator.pair.base.value;

        int i = 0;
        for (; i < keyColumns.size() - 1; ++i) {
          context.setKey
            (i + IndexDataBodyDepth,
             (Comparable) find(tree, keyColumns.get(i)).value);
        }

        context.delete
          (i + IndexDataBodyDepth,
           (Comparable) find(tree, keyColumns.get(i)).value);            
      } break;
      
      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  private static void updateIndexes(MyPatchContext context) {
    if (context.dirtyIndexes && context.indexBase != context.result) {
      if (context.indexUpdateBaseStack == null) {
        context.indexUpdateBaseStack = new NodeStack();
        context.indexUpdateForkStack = new NodeStack();
      }

      DiffIterator iterator = new DiffIterator
        (context.indexBase.root, context.indexUpdateBaseStack,
         context.result.root, context.indexUpdateForkStack,
         list(UnboundedEvaluatedInterval).iterator(), false);

      DiffPair pair = new DiffPair();

      while (iterator.next(pair)) {
        if (pair.fork != null) {
          for (NodeIterator indexKeys = new NodeIterator
                 (context.indexUpdateBaseStack, pathFind
                  (context.result.root, IndexTable, IndexTable.primaryKey.key,
                   (MyTable) pair.fork.key));
               indexKeys.hasNext();)
          {
            updateIndexTree
              (context, (IndexKey) indexKeys.next().key, context.indexBase,
               context.indexUpdateBaseStack, context.indexUpdateForkStack);
          }
        }
      }
    }

    context.dirtyIndexes = false;
    context.indexBase = context.result;
  }

  private static void updateIndex(MyPatchContext context,
                                  MyIndex index)
  {
    if (! equal(index.table.primaryKey.key, index.key)) {
      updateIndexes(context);
    }
  }

  private static void prepareForUpdate(MyPatchContext context,
                                       MyTable table)
  {
    // since we update non-primary-key indexes lazily, we may need to
    // freeze a copy of the last revision which contained up-to-date
    // indexes so we can do a diff later and use it to update them

    if (pathFind(context.result.root, IndexTable, IndexTable.primaryKey.key,
                 table) != NullNode)
    {
      context.dirtyIndexes = true;

      if (context.indexBase == context.result) {
        context.setToken(new Object());
      }
    }
  }

  private void treeDelete(MyPatchContext context,
                          Comparable[] keys)
  {
    if (keys.length == 0) {
      context.deleteAll();
      return;
    }

    MyTable table = (MyTable) keys[0];

    if (keys.length == 1) {
      context.delete(TableDataDepth, table);
      return;
    }

    prepareForUpdate(context, table);

    context.setKey(TableDataDepth, table);
    context.setKey(IndexDataDepth, table.primaryKey);

    int i = 0;
    for (; i < keys.length - 1; ++i) {
      context.setKey(i + IndexDataBodyDepth, keys[i]);
    }

    context.delete(i + IndexDataBodyDepth, keys[i]);
  }

  private void treeInsert(MyPatchContext context,
                          DuplicateKeyResolution duplicateKeyResolution,
                          MyTable table,
                          MyColumn column,
                          Object value,
                          Comparable[] path)
  {
    prepareForUpdate(context, table);

    context.setKey(TableDataDepth, table);
    context.setKey(IndexDataDepth, table.primaryKey);

    for (int i = 0; i < path.length; ++i) {
      context.setKey(i + IndexDataBodyDepth, path[i]);
    }

    Node n = context.blaze(path.length + IndexDataBodyDepth, column);

    if (n.value == NullNode) {
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

  private static class InsertTemplate extends MyPatchTemplate {
    public final MyTable table;
    public final List<MyColumn> columns;
    public final List<MyExpression> values;
    public final DuplicateKeyResolution duplicateKeyResolution;

    public InsertTemplate(int parameterCount,
                          MyTable table,
                          List<MyColumn> columns,
                          List<MyExpression> values,
                          DuplicateKeyResolution duplicateKeyResolution)
    {
      super(parameterCount);
      this.table = table;
      this.columns = columns;
      this.values = values;
      this.duplicateKeyResolution = duplicateKeyResolution;
    }

    public int apply(MyPatchContext context,
                     Object[] parameters)
    {
      ExpressionContext expressionContext = new ExpressionContext(parameters);

      Map<MyColumn, Object> map = new HashMap();
      { int index = 0;
        Iterator<MyColumn> columnIterator = columns.iterator();
        Iterator<MyExpression> valueIterator = values.iterator();
        while (columnIterator.hasNext()) {
          MyColumn column = columnIterator.next();
          Object value = valueIterator.next().makeLiveExpression
            (expressionContext).evaluate(false);

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
      
      Node tree = NullNode;
      BlazeResult result = new BlazeResult();
      for (MyColumn c: columns) {
        tree = blaze(result, context.token, context.stack, tree, c);
        result.node.value = map.get(c);
      }

      prepareForUpdate(context, table);

      IndexKey indexKey = table.primaryKey.key;

      context.setKey(TableDataDepth, table);
      context.setKey(IndexDataDepth, indexKey);

      List<MyColumn> columns = indexKey.index.columns;
      int i;
      for (i = 0; i < columns.size() - 1; ++i) {
        context.setKey
          (i + IndexDataBodyDepth, (Comparable) map.get(columns.get(i)));
      }

      Node n = context.blaze
        (i + IndexDataBodyDepth, (Comparable) map.get(columns.get(i)));

      if (n.value == NullNode) {
        n.value = tree;
        return 1;
      } else {
        switch (duplicateKeyResolution) {
        case Skip:
          return 0;

        case Overwrite:
          n.value = tree;
          return 1;

        case Throw:
          throw new DuplicateKeyException();

        default:
          throw new RuntimeException
            ("unexpected resolution: " + duplicateKeyResolution);
        }
      }
    }
  }

  private static class UpdateTemplate extends MyPatchTemplate {
    public final MyTableReference tableReference;
    public final MyExpression test;
    public final List<MyColumn> columns;
    public final List<MyExpression> values;

    public UpdateTemplate(int parameterCount,
                          MyTableReference tableReference,
                          MyExpression test,
                          List<MyColumn> columns,
                          List<MyExpression> values)
    {
      super(parameterCount);
      this.tableReference = tableReference;
      this.test = test;
      this.columns = columns;
      this.values = values;
    }

    public int apply(MyPatchContext context,
                     Object[] parameters)
    {
      ExpressionContext expressionContext = new ExpressionContext(parameters);
      
      LiveExpression liveTest = test.makeLiveExpression(expressionContext);

      List<LiveExpression> liveValues = new ArrayList(values.size());
      for (MyExpression e: values) {
        liveValues.add(e.makeLiveExpression(expressionContext));
      }

      MyTable table = tableReference.table;

      context.setKey(TableDataDepth, table);

      Plan plan = choosePlan
        (EmptyRevision, NullNodeStack, context.result, context.stack, liveTest,
         tableReference);

      updateIndex(context, plan.index);

      Object[] values = new Object[columns.size()];
      BlazeResult result = new BlazeResult();
      int count = 0;
      MyRevision revision = context.result;

      IndexKey indexKey = table.primaryKey.key;

      context.setKey(IndexDataDepth, indexKey);

      MyTableReference.MySourceIterator iterator = tableReference.iterator
        (EmptyRevision, NullNodeStack, revision, new NodeStack(), liveTest,
         expressionContext, plan, false);
                                                                            
      List<MyColumn> keyColumns = indexKey.index.columns;

      int[] keyColumnsUpdated;
      { List<MyColumn> columnList = new ArrayList();
        for (MyColumn c: keyColumns) {
          if (columns.contains(c)) {
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
            keyColumnsUpdated[i] = columns.indexOf(columnList.get(i));
          }
        }
      }
        
      Object deleteToken = indexKey.equals(plan.index.key)
        ? null : context.token;

      count = 0;
      boolean done = false;
      while (! done) {
        ResultType type = iterator.nextRow();
        switch (type) {
        case End:
          done = true;
          break;
      
        case Inserted: {
          prepareForUpdate(context, table);

          ++ count;

          for (int i = 0; i < columns.size(); ++i) {
            values[i] = liveValues.get(i).evaluate(false);
          }

          Node original = (Node) iterator.pair.fork.value;

          boolean keyValuesChanged = false;
          if (keyColumnsUpdated != null) {
            // some of the columns in the current index are being
            // updated, but we don't need to remove and reinsert the
            // row unless at least one is actually changing to a new
            // value
            for (int columnIndex: keyColumnsUpdated) {
              if (! equal(values[columnIndex], find
                          (original, keyColumns.get(columnIndex)).value))
              {
                keyValuesChanged = true;
                break;
              }
            }

            if (! keyValuesChanged) {
              break;
            }

            if (deleteToken == null) {
              context.setToken(deleteToken = new Object());
            }

            int i = 0;
            for (; i < keyColumns.size() - 1; ++i) {
              context.setKey
                (i + IndexDataBodyDepth,
                 (Comparable) find(original, keyColumns.get(i)).value);
            }

            context.delete
              (i + IndexDataBodyDepth,
               (Comparable) find(original, keyColumns.get(i)).value);
          }

          Node tree = original;

          for (int i = 0; i < columns.size(); ++i) {
            MyColumn column = columns.get(i);
            Object value = values[i];

            if (value != null && ! column.type.isInstance(value)) {
              throw new ClassCastException
                (value.getClass() + " cannot be cast to " + column.type);
            }

            if (value == null) {
              tree = delete(context.token, context.stack, tree, column);
            } else {
              tree = blaze
                (result, context.token, context.stack, tree, column);
              result.node.value = value;
            }
          }

          int i = 0;
          for (; i < keyColumns.size() - 1; ++i) {
            context.setKey
              (i + IndexDataBodyDepth,
               (Comparable) find(tree, keyColumns.get(i)).value);
          }

          Node n = context.blaze
            (i + IndexDataBodyDepth,
             (Comparable) find(tree, keyColumns.get(i)).value);

          if (n.value == NullNode || (! keyValuesChanged)) {
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

  private static class DeleteTemplate extends MyPatchTemplate {
    public final MyTableReference tableReference;
    public final MyExpression test;

    public DeleteTemplate(int parameterCount,
                          MyTableReference tableReference,
                          MyExpression test)
    {
      super(parameterCount);
      this.tableReference = tableReference;
      this.test = test;
    }

    public int apply(MyPatchContext context,
                     Object[] parameters)
    {
      ExpressionContext expressionContext = new ExpressionContext(parameters);

      LiveExpression liveTest = test.makeLiveExpression(expressionContext);

      context.setKey(TableDataDepth, tableReference.table);

      Plan plan = choosePlan
        (EmptyRevision, NullNodeStack, context.result, context.stack, liveTest,
         tableReference);

      updateIndex(context, plan.index);

      int count = 0;
      MyRevision revision = context.result;
      MyTable table = tableReference.table;
      IndexKey indexKey = table.primaryKey.key;

      context.setKey(IndexDataDepth, indexKey);

      MyTableReference.MySourceIterator iterator = tableReference.iterator
        (EmptyRevision, NullNodeStack, revision, new NodeStack(), liveTest,
         expressionContext, plan, false);

      List<MyColumn> keyColumns = indexKey.index.columns;

      Object deleteToken = indexKey.equals(plan.index.key)
        ? null : context.token;

      count = 0;
      boolean done = false;
      while (! done) {
        ResultType type = iterator.nextRow();
        switch (type) {
        case End:
          done = true;
          break;
      
        case Inserted: {
          prepareForUpdate(context, table);

          ++ count;

          if (deleteToken == null) {
            context.setToken(deleteToken = new Object());
          }

          Node tree = (Node) iterator.pair.fork.value;

          int i = 0;
          for (; i < keyColumns.size() - 1; ++i) {
            context.setKey
              (i + IndexDataBodyDepth,
               (Comparable) find(tree, keyColumns.get(i)).value);
          }

          context.delete
            (i + IndexDataBodyDepth,
             (Comparable) find(tree, keyColumns.get(i)).value);
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }

      return count;
    }
  }

  private static interface ExpressionVisitor {
    public void visit(Expression e);
  }

  private static class ParameterCounter implements ExpressionVisitor {
    public final Set<Parameter> parameters = new HashSet();
    public int count;

    public void visit(Expression e) {
      if (e instanceof Parameter) {
        Parameter pe = (Parameter) e;
        if (parameters.contains(pe)) {
          throw new IllegalArgumentException
            ("duplicate parameter expression");
        } else {
          parameters.add(pe);
          ++ count;
        }
      }
    }
  }

  private static class ColumnReferenceFinder implements ExpressionVisitor {
    public final MyTableReference tableReference;
    public final MyColumn column;
    public LiveColumnReference reference;

    public ColumnReferenceFinder(MyTableReference tableReference,
                                 MyColumn column)
    {
      this.tableReference = tableReference;
      this.column = column;
    }

    public void visit(Expression e) {
      if (e instanceof LiveColumnReference) {
        LiveColumnReference r = (LiveColumnReference) e;
        if (r.tableReference == tableReference
            && r.column == column)
        {
          reference = r;
        }
      }
    }
  }

  public Column column(Class type) {
    return new MyColumn(type);
  }

  public Index index(Table table, List<Column> columns) {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    List copyOfColumns = new ArrayList(columns);

    for (Object c: copyOfColumns) {
      if (! (c instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    // pad index with primary key columns to make it unique
    for (MyColumn c: myTable.primaryKey.columns) {
      if (! copyOfColumns.contains(c)) {
        copyOfColumns.add(c);
      }
    }

    if (copyOfColumns.size() > MaxIndexDataBodyDepth) {
      throw new IllegalArgumentException
        ("too many columns in index (maximum is " + MaxIndexDataBodyDepth
         + "; got " + copyOfColumns.size() + ")");
    }

    return new MyIndex(myTable, copyOfColumns);
  }

  public Table table(List<Column> primaryKey) {
    List copyOfPrimaryKey = new ArrayList(primaryKey);

    for (Object c: copyOfPrimaryKey) {
      if (! (c instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    return new MyTable(copyOfPrimaryKey);
  }

  public Revision revision() {
    return EmptyRevision;
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
    return new Constant(value);
  }

  public Expression parameter() {
    return new Parameter();
  }

  public Expression operation(BinaryOperationType type,
                              Expression left,
                              Expression right)
  {
    MyExpression myLeft;
    MyExpression myRight;
    try {
      myLeft = (MyExpression) left;
      myRight = (MyExpression) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    switch (type.operationClass()) {
    case Comparison:
      return new Comparison(type, myLeft, myRight);

    case Boolean:
      return new BooleanBinaryOperation(type, myLeft, myRight);

    default: throw new RuntimeException("unexpected operation type: " + type);
    }
  }

  public Expression operation(UnaryOperationType type,
                              Expression operand)
  {
    MyExpression myOperand;
    try {
      myOperand = (MyExpression) operand;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    switch (type.operationClass()) {
    case Boolean:
      return new BooleanUnaryOperation(type, myOperand);

    default: throw new RuntimeException("unexpected operation type: " + type);
    }
  }

  public Source join(JoinType type,
                     Source left,
                     Source right)
  {
    MySource myLeft;
    MySource myRight;
    try {
      myLeft = (MySource) left;
      myRight = (MySource) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table reference not created by this implementation");
    }

    return new Join(type, myLeft, myRight);
  }

  public QueryTemplate queryTemplate(List<Expression> expressions,
                                     Source source,
                                     Expression test)
  {
    List copyOfExpressions = new ArrayList(expressions);

    ParameterCounter counter = new ParameterCounter();

    for (Object expression: copyOfExpressions) {
      MyExpression myExpression;
      try {
        myExpression = (MyExpression) expression;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(counter);
    }

    MySource mySource;
    try {
      mySource = (MySource) source;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("source not created by this implementation");        
    }

    MyExpression myTest;
    try {
      myTest = (MyExpression) test;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("expression not created by this implementation");
    }

    myTest.visit(counter);

    return new MyQueryTemplate
      (counter.count, copyOfExpressions, mySource, myTest);
  }

  public QueryResult diff(Revision base,
                          Revision fork,
                          QueryTemplate template,
                          Object ... parameters)
  {
    MyRevision myBase;
    MyRevision myFork;
    try {
      myBase = (MyRevision) base;
      myFork = (MyRevision) fork;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    MyQueryTemplate myTemplate;
    try {
      myTemplate = (MyQueryTemplate) template;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("query template not created by this implementation");
    }

    if (parameters.length != myTemplate.parameterCount) {
      throw new IllegalArgumentException
        ("wrong number of parameters (expected "
         + myTemplate.parameterCount + "; got "
         + parameters.length + ")");
    }

    return new MyQueryResult(myBase, myFork, myTemplate, copy(parameters));
  }

  public DiffResult diff(Revision base,
                         Revision fork)
  {
    MyRevision myBase;
    MyRevision myFork;
    try {
      myBase = (MyRevision) base;
      myFork = (MyRevision) fork;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");        
    }

    return new MyDiffResult(myBase, new NodeStack(), myFork, new NodeStack());
  }

  public PatchTemplate insertTemplate
    (Table table,
     List<Column> columns,
     List<Expression> values,
     DuplicateKeyResolution duplicateKeyResolution)
  {
    MyTable myTable;
    try {
      myTable = (MyTable) table;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    ParameterCounter counter = new ParameterCounter();

    List copyOfColumns = new ArrayList(columns);
    List copyOfValues = new ArrayList(values);

    if (copyOfColumns.size() != copyOfValues.size()) {
      throw new IllegalArgumentException
        ("column and value lists must be of equal length");
    }

    Set set = new HashSet(myTable.primaryKey.columns);
    for (Object o: copyOfColumns) {
      if (! (o instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }

      set.remove(o);
    }

    if (set.size() != 0) {
      throw new IllegalArgumentException
        ("not enough columns specified to satisfy primary key");
    }

    for (Object o: copyOfValues) {
      MyExpression myExpression;
      try {
        myExpression = (MyExpression) o;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(counter);
    }

    return new InsertTemplate
      (counter.count, myTable, copyOfColumns, copyOfValues,
       duplicateKeyResolution);
  }

  public PatchTemplate updateTemplate
    (TableReference tableReference,
     Expression test,
     List<Column> columns,
     List<Expression> values)
  {
    MyTableReference myTableReference;
    try {
      myTableReference = (MyTableReference) tableReference;
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

    ParameterCounter counter = new ParameterCounter();

    myTest.visit(counter);

    List copyOfColumns = new ArrayList(columns);
    List copyOfValues = new ArrayList(values);

    if (copyOfColumns.size() != copyOfValues.size()) {
      throw new IllegalArgumentException
        ("column and value lists must be of equal length");
    }

    for (Object o: copyOfColumns) {
      if (! (o instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    for (Object o: copyOfValues) {
      MyExpression myExpression;
      try {
        myExpression = (MyExpression) o;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("expression not created by this implementation");
      }
      
      myExpression.visit(counter);
    }

    return new UpdateTemplate
      (counter.count, myTableReference, myTest, copyOfColumns, copyOfValues);
  }

  public PatchTemplate deleteTemplate(TableReference tableReference,
                                      Expression test)
  {
    MyTableReference myTableReference;
    try {
      myTableReference = (MyTableReference) tableReference;
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

    ParameterCounter counter = new ParameterCounter();

    myTest.visit(counter);

    return new DeleteTemplate(counter.count, myTableReference, myTest);
  }

  public PatchContext patchContext(Revision base) {
    MyRevision myBase;
    try {
      myBase = (MyRevision) base;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return new MyPatchContext(new Object(), myBase, new NodeStack());
  }

  public int apply(PatchContext context,
                   PatchTemplate template,
                   Object ... parameters)
  {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    if (myContext.token == null) {
      throw new IllegalStateException("patch context already committed");
    }

    try {
      MyPatchTemplate myTemplate;
      try {
        myTemplate = (MyPatchTemplate) template;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("patch template not created by this implementation");        
      }

      if (parameters.length != myTemplate.parameterCount) {
        throw new IllegalArgumentException
          ("wrong number of parameters (expected "
           + myTemplate.parameterCount + "; got "
           + parameters.length + ")");
      }

      return myTemplate.apply(myContext, copy(parameters));
    } catch (RuntimeException e) {
      myContext.token = null;
      throw e;
    }
  }

  public void treeDelete(PatchContext context,
                         Object ... path)
  {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    Comparable[] myPath = new Comparable[path.length];
    for (int i = 0; i < path.length; ++i) {
      myPath[i] = (Comparable) path[i];
    }
    
    treeDelete(myContext, myPath);
  }

  public void treeInsert(PatchContext context,
                         DuplicateKeyResolution duplicateKeyResolution,
                         Object ... path)
  {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    MyTable myTable;
    try {
      myTable = (MyTable) path[0];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("table not created by this implementation");        
    }

    List<MyColumn> columns = myTable.primaryKey.columns;

    if (path.length < columns.size() + 3) {
      throw new IllegalArgumentException
        ("too few parameters specified for primary key");
    }

    MyColumn myColumn;
    try {
      myColumn = (MyColumn) path[columns.size() + 1];
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("column not created by this implementation");        
    }

    Comparable[] myPath = new Comparable[columns.size()];
    for (int i = 0; i < myPath.length; ++i) {
      Comparable c = (Comparable) path[i + 1];
      if (columns.get(i) == myColumn) {
        throw new IllegalArgumentException
          ("cannot use treeInsert to update a primary key column");        
      }
      myPath[i] = c;
    }

    Object value = path[columns.size() + 2];
    if (value != null && ! myColumn.type.isInstance(value)) {
      throw new ClassCastException
        (value.getClass() + " cannot be cast to " + myColumn.type);
    }

    treeInsert
      (myContext, duplicateKeyResolution, myTable, myColumn, value, myPath);
  }

  public void add(PatchContext context,
                  Index index)
  {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    if (myContext.token == null) {
      throw new IllegalStateException("patch context already committed");
    }

    try {
      MyIndex myIndex;
      try {
        myIndex = (MyIndex) index;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("index not created by this implementation");        
      }

      addIndex(myContext, myIndex);
    } catch (RuntimeException e) {
      myContext.token = null;
      throw e;
    }
  }

  public void remove(PatchContext context,
                     Index index)
  {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    if (myContext.token == null) {
      throw new IllegalStateException("patch context already committed");
    }

    try {
      MyIndex myIndex;
      try {
        myIndex = (MyIndex) index;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException
          ("index not created by this implementation");        
      }

      removeIndex(myContext, myIndex);
    } catch (RuntimeException e) {
      myContext.token = null;
      throw e;
    }
  }

  public Revision commit(PatchContext context) {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

    if (myContext.token == null) {
      throw new IllegalStateException("patch context already committed");
    }

    updateIndexes(myContext);

    myContext.token = null;

    return myContext.result;
  }

  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver)
  {
    MyRevision myBase;
    MyRevision myLeft;
    MyRevision myRight;
    try {
      myBase = (MyRevision) base;
      myLeft = (MyRevision) left;
      myRight = (MyRevision) right;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("revision not created by this implementation");
    }

    return mergeRevisions(myBase, myLeft, myRight, conflictResolver);
  }

  private static Object[] copy(Object[] array) {
    Object[] copy = new Object[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    return copy;
  }

  private static MyRevision mergeRevisions
    (MyRevision base,
     MyRevision left,
     MyRevision right,
     ConflictResolver conflictResolver)
  {
    // The merge builds a new revision starting with the specified
    // left revision via a process which consists of the following
    // steps:
    //
    //  1. Merge the primary key data trees of each table and delete
    //     obsolete index data trees.
    //
    //  2. Update non-primary-key data trees, removing obsolete rows
    //     and adding new or updated ones.
    //
    //  3. Build data trees for any new indexes added.

    MyPatchContext context = new MyPatchContext
      (new Object(), left, new NodeStack());

    List<IndexKey> indexKeys = new ArrayList();
    List<IndexKey> newIndexKeys = new ArrayList();

    NodeStack baseStack = new NodeStack();
    NodeStack leftStack = new NodeStack();
    NodeStack rightStack = new NodeStack();

    { MergeIterator[] iterators = new MergeIterator[MaxDepth + 1];
    
      iterators[0] = new MergeIterator
        (base.root, baseStack, left.root, leftStack, right.root, rightStack);

      int depth = 0;
      int bottom = -1;
      MyTable table = null;
      MergeTriple triple = new MergeTriple();

      // merge primary key data trees of each table, deleting obsolete
      // rows from any other index data trees as we go
      while (true) {
        if (iterators[depth].next(triple)) {
          expect(triple.base != NullNode
                 && triple.left != NullNode
                 && triple.right != NullNode);

          boolean descend = false;
          boolean conflict = false;
          if (triple.base == null) {
            if (triple.left == null) {
              context.insertOrUpdate
                (depth, triple.right.key, triple.right.value);
            } else if (triple.right == null) {
              // do nothing -- left already has insert
            } else if (depth == bottom) {
              if (equal(triple.left.value, triple.right.value)) {
                // do nothing -- inserts match and left already has it
              } else {
                conflict = true;
              }
            } else {
              descend = true;
            }
          } else if (triple.left != null) {
            if (triple.right != null) {
              if (triple.left == triple.base) {
                context.insertOrUpdate
                  (depth, triple.right.key, triple.right.value);
              } else if (triple.right == triple.base) {
                // do nothing -- left already has update
              } else if (depth == bottom) {
                if (equal(triple.left.value, triple.right.value)
                    || equal(triple.base.value, triple.right.value))
                {
                  // do nothing -- updates match or only left changed,
                  // and left already has it
                } else if (equal(triple.base.value, triple.left.value)) {
                  context.insertOrUpdate
                    (depth, triple.right.key, (Node) triple.right.value);
                } else {
                  conflict = true;
                }
              } else {
                descend = true;
              }
            } else {
              context.delete(depth, triple.left.key);
            }
          } else {
            // do nothing -- left already has delete
          }

          if (conflict) {
            Object[] primaryKeyValues = new Object[depth - IndexDataBodyDepth];
            for (int i = 0; i < primaryKeyValues.length; ++i) {
              primaryKeyValues[i] = context.keys[i + IndexDataBodyDepth];
            }

            Object result = conflictResolver.resolveConflict
              (table,
               (Column) triple.left.key,
               primaryKeyValues,
               triple.base == null ? null : triple.base.value,
               triple.left.value,
               triple.right.value);

            if (equal(result, triple.left.value)) {
              // do nothing -- left already has insert
            } else if (result == null) {
              context.delete(depth, triple.left.key);
            } else {
              context.insertOrUpdate(depth, triple.left.key, result);
            }
          } else if (descend) {
            if (depth == TableDataDepth) {
              table = (MyTable) triple.left.key;

              if (table != IndexTable) {
                DiffIterator indexIterator = new DiffIterator
                  (pathFind(left.root, IndexTable, IndexTable.primaryKey.key,
                            table),
                   baseStack = new NodeStack(baseStack),
                   pathFind(context.result.root, IndexTable,
                            IndexTable.primaryKey.key, table),
                   leftStack = new NodeStack(leftStack),
                   list(UnboundedEvaluatedInterval).iterator(),
                   true);
          
                DiffPair pair = new DiffPair();
                while (indexIterator.next(pair)) {
                  if (pair.base != null) {
                    if (pair.fork != null) {
                      IndexKey indexKey = (IndexKey) pair.base.key;
                      if (! indexKey.equals(table.primaryKey.key)) {
                        indexKeys.add(indexKey);
                      }
                    } else {
                      context.setKey(TableDataDepth, table);
                      context.delete(IndexDataDepth, (IndexKey) pair.base.key);
                    }
                  } else if (pair.fork != null) {
                    IndexKey indexKey = (IndexKey) pair.fork.key;
                    if (! indexKey.equals(table.primaryKey.key)) {
                      newIndexKeys.add(indexKey);
                    }
                  }
                }

                popStack(baseStack);
                popStack(leftStack);
              }
            } else if (depth == IndexDataDepth) {
              IndexKey indexKey = (IndexKey) triple.left.key;
              if (equal(indexKey, table.primaryKey.key)) {
                bottom = indexKey.index.columns.size() + IndexDataBodyDepth;
              } else {
                // skip non-primary-key index data trees -- we'll handle
                // those later
                continue;
              }
            }

            context.setKey(depth, triple.left.key);
          
            ++ depth;

            iterators[depth] = new MergeIterator
              (triple.base == null ? NullNode : (Node) triple.base.value,
               baseStack = new NodeStack(baseStack),
               (Node) triple.left.value,
               leftStack = new NodeStack(leftStack),
               (Node) triple.right.value,
               rightStack = new NodeStack(rightStack));
          }
        } else if (depth == 0) {
          break;
        } else {
          iterators[depth] = null;

          -- depth;

          baseStack = popStack(baseStack);
          leftStack = popStack(leftStack);
          rightStack = popStack(rightStack);
        }
      }
    }

    // Update non-primary-key data trees
    for (IndexKey indexKey: indexKeys) {
      updateIndexTree(context, indexKey, left, leftStack, baseStack);
    }

    // build data trees for any new index keys
    for (IndexKey indexKey: newIndexKeys) {
      buildIndexTree(context, indexKey.index);
    }

    return context.result;
  }

  private static Node find(Node n, Comparable key) {
    while (n != NullNode) {
      int difference = compare(key, n.key);
      if (difference < 0) {
        n = n.left;
      } else if (difference > 0) {
        n = n.right;
      } else {
        return n;
      }
    }
    return NullNode;
  }

  private static Node leftRotate(Object token, Node n) {
    Node child = getNode(token, n.right);
    n.right = child.left;
    child.left = n;
    return child;
  }

  private static Node rightRotate(Object token, Node n) {
    Node child = getNode(token, n.left);
    n.left = child.right;
    child.right = n;
    return child;
  }

  private static class BlazeResult {
    public Node node;
  }

  private static Node blaze(BlazeResult result,
                            Object token,
                            NodeStack stack,
                            Node root,
                            Comparable key)
  {
    if (root == null) {
      root = NullNode;
    }

    stack = new NodeStack(stack);
    Node newRoot = getNode(token, root);

    Node old = root;
    Node new_ = newRoot;
    while (old != NullNode) {
      int difference = compare(key, old.key);
      if (difference < 0) {
        push(stack, new_);
        old = old.left;
        new_ = new_.left = getNode(token, old);
      } else if (difference > 0) {
        push(stack, new_);
        old = old.right;
        new_ = new_.right = getNode(token, old);
      } else {
        result.node = new_;
        popStack(stack);
        return newRoot;
      }
    }

    new_.key = key;
    result.node = new_;

    // rebalance
    new_.red = true;

    while (stack.top != null && stack.top.red) {
      if (stack.top == peek(stack).left) {
        if (peek(stack).right.red) {
          stack.top.red = false;
          peek(stack).right = getNode(token, peek(stack).right);
          peek(stack).right.red = false;
          peek(stack).red = true;
          new_ = peek(stack);
          pop(stack, 2);
        } else {
          if (new_ == stack.top.right) {
            new_ = stack.top;
            pop(stack);

            Node n = leftRotate(token, new_);
            if (stack.top.right == new_) {
              stack.top.right = n;
            } else {
              stack.top.left = n;
            }
            push(stack, n);
          }
          stack.top.red = false;
          peek(stack).red = true;

          Node n = rightRotate(token, peek(stack));
          if (stack.index <= stack.base + 1) {
            newRoot = n;
          } else if (peek(stack, 1).right == peek(stack)) {
            peek(stack, 1).right = n;
          } else {
            peek(stack, 1).left = n;
          }
          // done
        }
      } else {
        // this is just the above code with left and right swapped:
        if (peek(stack).left.red) {
          stack.top.red = false;
          peek(stack).left = getNode(token, peek(stack).left);
          peek(stack).left.red = false;
          peek(stack).red = true;
          new_ = peek(stack);
          pop(stack, 2);
        } else {
          if (new_ == stack.top.left) {
            new_ = stack.top;
            pop(stack);

            Node n = rightRotate(token, new_);
            if (stack.top.right == new_) {
              stack.top.right = n;
            } else {
              stack.top.left = n;
            }
            push(stack, n);
          }
          stack.top.red = false;
          peek(stack).red = true;

          Node n = leftRotate(token, peek(stack));
          if (stack.index <= stack.base + 1) {
            newRoot = n;
          } else if (peek(stack, 1).right == peek(stack)) {
            peek(stack, 1).right = n;
          } else {
            peek(stack, 1).left = n;
          }
          // done
        }
      }
    }

    newRoot.red = false;

    popStack(stack);
    return newRoot;
  }

  private static void minimum(Object token,
                              Node n,
                              NodeStack stack)
  {
    while (n.left != NullNode) {
      n.left = getNode(token, n.left);
      push(stack, n);
      n = n.left;
    }

    push(stack, n);
  }

  private static void successor(Object token,
                                Node n,
                                NodeStack stack)
  {
    if (n.right != NullNode) {
      n.right = getNode(token, n.right);
      push(stack, n);
      minimum(token, n.right, stack);
    } else {
      while (stack.top != null && n == stack.top.right) {
        n = stack.top;
        pop(stack);
      }
    }
  }

  private static Node delete(Object token,
                             NodeStack stack,
                             Node root,
                             Comparable key)
  {
    if (root == NullNode) {
      return root;
    } else if (root.left == NullNode && root.right == NullNode) {
      if (equal(key, root.key)) {
        return NullNode;
      } else {
        return root;
      }
    }

    stack = new NodeStack(stack);
    Node newRoot = getNode(token, root);

    Node old = root;
    Node new_ = newRoot;
    while (old != NullNode) {
      int difference = compare(key, old.key);
      if (difference < 0) {
        push(stack, new_);
        old = old.left;
        new_ = new_.left = getNode(token, old);
      } else if (difference > 0) {
        push(stack, new_);
        old = old.right;
        new_ = new_.right = getNode(token, old);
      } else {
        break;
      }
    }

    if (old == NullNode) {
      popStack(stack);
      return root;
    }

    Node dead;
    if (new_.left == NullNode || new_.right == NullNode) {
      dead = new_;
    } else {
      successor(token, new_, stack);
      dead = stack.top;
      pop(stack);
    }
    
    Node child;
    if (dead.left != NullNode) {
      child = getNode(token, dead.left);
    } else if (dead.right != NullNode) {
      child = getNode(token, dead.right);
    } else {
      child = NullNode;
    }

    if (stack.top == null) {
      child.red = false;
      popStack(stack);
      return child;
    } else if (dead == stack.top.left) {
      stack.top.left = child;
    } else {
      stack.top.right = child;
    }

    if (dead != new_) {
      new_.key = dead.key;
      new_.value = dead.value;
    }

    if (! dead.red) {
      // rebalance
      while (stack.top != null && ! child.red) {
        if (child == stack.top.left) {
          Node sibling = stack.top.right = getNode(token, stack.top.right);
          if (sibling.red) {
            expect(sibling.token == token);
            sibling.red = false;
            stack.top.red = true;
            
            Node n = leftRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (peek(stack).right == stack.top) {
              peek(stack).right = n;
            } else {
              peek(stack).left = n;
            }
            Node parent = stack.top;
            stack.top = n;
            push(stack, parent);

            sibling = stack.top.right;
          }

          if (! (sibling.left.red || sibling.right.red)) {
            sibling.red = true;
            child = stack.top;
            pop(stack);
          } else {
            if (! sibling.right.red) {
              sibling.left = getNode(token, sibling.left);
              sibling.left.red = false;

              sibling.red = true;
              sibling = stack.top.right = rightRotate(token, sibling);
            }

            sibling.red = stack.top.red;
            stack.top.red = false;

            sibling.right = getNode(token, sibling.right);
            sibling.right.red = false;
            
            Node n = leftRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (peek(stack).right == stack.top) {
              peek(stack).right = n;
            } else {
              peek(stack).left = n;
            }

            child = newRoot;
            clear(stack);
          }
        } else {
          // this is just the above code with left and right swapped:
          Node sibling = stack.top.left = getNode(token, stack.top.left);
          if (sibling.red) {
            expect(sibling.token == token);
            sibling.red = false;
            stack.top.red = true;
            
            Node n = rightRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (peek(stack).left == stack.top) {
              peek(stack).left = n;
            } else {
              peek(stack).right = n;
            }
            Node parent = stack.top;
            stack.top = n;
            push(stack, parent);

            sibling = stack.top.left;
          }

          if (! (sibling.right.red || sibling.left.red)) {
            sibling.red = true;
            child = stack.top;
            pop(stack);
          } else {
            if (! sibling.left.red) {
              sibling.right = getNode(token, sibling.right);
              sibling.right.red = false;

              sibling.red = true;
              sibling = stack.top.left = leftRotate(token, sibling);
            }

            sibling.red = stack.top.red;
            stack.top.red = false;

            sibling.left = getNode(token, sibling.left);
            sibling.left.red = false;
            
            Node n = rightRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (peek(stack).left == stack.top) {
              peek(stack).left = n;
            } else {
              peek(stack).right = n;
            }

            child = newRoot;
            clear(stack);
          }
        }
      }

      expect(child.token == token);
      child.red = false;
    }

    popStack(stack);
    return newRoot;
  }

  private static class NodeIterator {
    public final NodeStack stack;
    public boolean hasNext;
    
    public NodeIterator(NodeStack stack,
                        Node root)
    {
      if (root != NullNode) {
        this.stack = new NodeStack(stack);
        push(this.stack, root);
        leftmost(this.stack);
        hasNext = true;
      } else {
        this.stack = null;
        hasNext = false;
      }
    }

    public boolean hasNext() {
      return hasNext;
    }

    public Node next() {
      if (! hasNext) {
        throw new NoSuchElementException();
      }

      Node n = stack.top;
      MyDBMS.next(stack);
      
      if (stack.top == null) {
        popStack(stack);
        hasNext = false;
      }

      return n;
    }
  }

  private static void dump(Node node, java.io.PrintStream out, int depth) {
    if (node == NullNode) {
      return;
    } else {
      dump(node.left, out, depth + 1);

      for (int i = 0; i < depth; ++i) {
        out.print("  ");
      }
      out.print(node.red ? "(r) " : "(b) ");
      if (node.value instanceof Node) {
        out.println(node.key + ": subtree");
        dump((Node) node.value, out, depth + 2);
      } else {
        out.println(node.key + ": " + node.value);
      }

      dump(node.right, out, depth + 1);
    }
  }
}
