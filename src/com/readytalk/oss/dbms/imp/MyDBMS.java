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

  private static final int TableDepth = 0;
  private static final int IndexDepth = 1;
  private static final int IndexBodyDepth = 2;
  private static final int MaxIndexDepth = 8;
  private static final int MaxDepth = IndexBodyDepth + MaxIndexDepth;
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

  private static class MyColumn implements Column {
    public final Class type;
    public final int id;

    public MyColumn(Class type) {
      this.type = type;
      this.id = nextId();
    }
  }

  private static class MyIndex implements Index, Comparable<MyIndex> {
    public final List<MyColumn> columns;
    public final boolean unique;
    public final int id;

    public MyIndex(List<MyColumn> columns, boolean unique) {
      this.columns = columns;
      this.unique = unique;
      this.id = nextId();
    }

    public int compareTo(MyIndex o) {
      return id - o.id;
    }
  }

  private static class MyTable implements Table, Comparable<MyTable> {
    public final Collection<MyColumn> columns;
    public final Collection<MyIndex> indexes;
    public final MyIndex primaryKey;
    public final int id;

    public MyTable(Collection<MyColumn> columns,
                   Collection<MyIndex> indexes,
                   MyIndex primaryKey)
    {
      this.columns = columns;
      this.indexes = indexes;
      this.primaryKey = primaryKey;
      this.id = nextId();
    }

    public int compareTo(MyTable o) {
      return id - o.id;
    }
  }

  private static class MyRow implements Row {
    public final Map<MyColumn, Object> map = new HashMap();

    public MyRow(MyTable table, Object[] tuple) {
      int i = 0;
      for (MyColumn c: table.columns) {
        map.put(c, tuple[i++]);
      }
    }

    public Object value(Column column) {
      return map.get(column);
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

  private static boolean equal(Object[] left,
                               Object[] right)
  {
    if (left == right) {
      return true;
    } else {
      for (int i = 0; i < left.length; ++i) {
        if (! equal(left[i], right[i])) {
          return false;
        }
      }
      return true;
    }
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
    public Object evaluate(boolean convertDummyToNull);
    public Scan makeScan(LiveColumnReference reference);
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

  private static void next(NodeStack stack) {
    Node then = stack.top;

    if (stack.top.right != NullNode) {
      push(stack, stack.top.right);

      while (stack.top.left != NullNode) {
        push(stack, stack.top.left);
      }
    } else {
      ascendNext(stack);
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
    push(s, s.top.left);
  }

  private static int compareForMerge(Node a, Node b) {
    if (a == null) {
      if (b == null) {
        return 0;
      } else {
        return 1;
      }
    } else if (b == null) {
      return -1;
    } else {
      return compare(a.key, b.key);
    }
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

      push(base, baseRoot);
      push(left, leftRoot);
      push(right, rightRoot);

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
                       && base.top.left == NullNode)
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
          } else {
            // left = right = base
            if (left.top == right.top && left.top == base.top) {
              if (left.top == null) {
                return false;
              } else {
                // no need to go any deeper -- there aren't any changes
                ascendNext(left);
                ascendNext(right);
                ascendNext(base);
                continue;
              }
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
                              boolean visitUnchanged)
      {
        this.base = (Node) find(base.root, table).value;
        this.fork = (Node) find(fork.root, table).value;
        this.test = test;
        this.expressionContext = expressionContext;
        this.visitUnchanged = visitUnchanged;

        Plan best = null;

        for (MyIndex index: table.indexes) {
          Plan plan = new Plan(index);

          for (int i = 0; i < plan.size; ++i) {
            MyColumn column = index.columns.get(i);

            LiveColumnReference reference = findColumnReference
              (expressionContext, MyTableReference.this, column);

            if (reference != null) {
              Scan scan = test.makeScan(reference);

              plan.scans[i] = scan;

              if (! scan.isUseful()) {
                plan.match = true;
              }

              reference.value = Dummy;
              plan.references[i] = reference;
            } else {
              plan.scans[i] = UnboundedInterval;              
            }

            if (! plan.match) {
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
        }

        this.plan = best;

        plan.iterators[0] = new DiffIterator
          ((Node) find(this.base, plan.index).value,
           this.baseStack = new NodeStack(baseStack),
           (Node) find(this.fork, plan.index).value,
           this.forkStack = new NodeStack(forkStack),
           plan.scans[0].evaluate().iterator(),
           visitUnchanged);

        { int i = 0;
          for (MyColumn column: table.columns) {
            LiveColumnReference reference = findColumnReference
              (expressionContext, MyTableReference.this, column);

            // skip references which will be populated as part of the
            // index scan:
            for (int j = 0; j < plan.size - 1; ++j) {
              if (reference == plan.references[j]) {
                reference = null;
                break;
              }
            }

            if (reference != null) {
              reference.index = i;
              columnReferences.add(reference);
            }

            ++i;
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
                           || equal((Object[]) pair.base.value,
                                    (Object[]) pair.fork.value))
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

      private boolean test(Node node) {
        if (node != null) {
          Object[] tuple = (Object[]) node.value;
        
          for (LiveColumnReference r: columnReferences) {
            r.value = tuple[r.index];
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
                                     boolean visitUnchanged)
    {
      return new MySourceIterator
        (base, baseStack, fork, forkStack, test, expressionContext,
         visitUnchanged);
    }

    public void visit(SourceVisitor visitor) {
      visitor.visit(this);
    }

    public void visit(ExpressionContext expressionContext,
                      ColumnReferenceVisitor visitor)
    {
      for (MyColumn column: table.columns) {
        LiveColumnReference reference = findColumnReference
          (expressionContext, this, column);

        if (reference != null) {
          visitor.visit(reference);
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
    public int index;
    public Object value = Undefined;

    public LiveColumnReference(MyTableReference tableReference,
                               MyColumn column)
    {
      this.tableReference = tableReference;
      this.column = column;
    }

    public Object evaluate(boolean convertDummyToNull) {
      return convertDummyToNull && value == Dummy ? null : value ;
    }

    public Scan makeScan(LiveColumnReference reference) {
      throw new UnsupportedOperationException();
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
  }

  private static abstract class MyPatchTemplate implements PatchTemplate {
    public final int parameterCount;

    public MyPatchTemplate(int parameterCount) {
      this.parameterCount = parameterCount;
    }

    public abstract int apply(MyPatchContext context,
                              Object[] parameters);
  }

  // todo: teach this about updating multiple indexes:
  private static class MyPatchContext implements PatchContext {
    public Object token;
    public final NodeStack stack;
    public final Comparable[] keys;
    public final Node[] blazedRoots;
    public final Node[] blazedLeaves;
    public final Node[] found;
    public final BlazeResult blazeResult = new BlazeResult();
    public MyRevision result;
    public int max;

    public MyPatchContext(Object token,
                          MyRevision result,
                          NodeStack stack)
    {
      this.token = token;
      this.result = result;
      this.stack = stack;
      keys = new Comparable[MaxDepth + 1];
      blazedRoots = new Node[MaxDepth + 1];
      blazedLeaves = new Node[MaxDepth + 1];
      found = new Node[MaxDepth + 1];
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

    public void insertOrUpdate(int index, Comparable key, Object value) {
      setKey(index, key);
      blaze(index).value = value;
    }

    public void delete(int index, Comparable key) {
      setKey(index, key);
      delete(index);
    }

    private void delete(int index) {
      expect(blazedLeaves[index] == null);
      Node root = blazedRoots[index];
      if (root == null) {
        if (index == 0) {
          root = MyDBMS.delete(token, stack, result.root, keys[0]);

          if (root != result.root) {
            result = getRevision(token, result, root);
          }
        } else {
          Node original = find(index);
          Node parent = find(index - 1);

          if (original == NullNode) {
            throw new RuntimeException();
          } else if (original == parent
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
    }

    private Node find(int index) {
      Node root = blazedRoots[index];
      if (root == null) {
        root = found[index];
        if (root == null) {
          if (index == 0) {
            root = MyDBMS.find(result.root, keys[0]);
            found[0] = root;
          } else {
            root = MyDBMS.find((Node) find(index - 1).value, keys[index]);
            found[index] = root;
          }
        }
      }
      return root;
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

  private static class InsertTemplate extends MyPatchTemplate {
    public final MyTable table;
    public final List<MyColumn> columns;
    public final List<MyExpression> values;
    public final boolean updateOnDuplicateKey;

    public InsertTemplate(int parameterCount,
                          MyTable table,
                          List<MyColumn> columns,
                          List<MyExpression> values,
                          boolean updateOnDuplicateKey)
    {
      super(parameterCount);
      this.table = table;
      this.columns = columns;
      this.values = values;
      this.updateOnDuplicateKey = updateOnDuplicateKey;
    }

    public int apply(MyPatchContext context,
                     Object[] parameters)
    {
      ExpressionContext expressionContext = new ExpressionContext(parameters);

      Map<MyColumn, LiveExpression> map = new HashMap();
      Iterator<MyColumn> columnIterator = columns.iterator();
      Iterator<MyExpression> valueIterator = values.iterator();
      while (columnIterator.hasNext()) {
        map.put(columnIterator.next(),
                valueIterator.next().makeLiveExpression(expressionContext));
      }
      
      Object[] tuple = new Object[table.columns.size()];
      { int i = 0;
        for (MyColumn c: table.columns) {
          tuple[i++] = map.get(c).evaluate(false);
        }
      }

      context.setKey(0, table);
      context.setKey(1, table.primaryKey);

      List<MyColumn> columns = table.primaryKey.columns;
      int i;
      for (i = 0; i < columns.size() - 1; ++i) {
        context.setKey
          (i + IndexBodyDepth,
           (Comparable) map.get(columns.get(i)).evaluate(false));
      }

      // todo: throw duplicate key exception if applicable and
      // updateOnDuplicateKey is false
      context.insertOrUpdate
        (i + IndexBodyDepth,
         (Comparable) map.get(columns.get(i)).evaluate(false), tuple);

      return 1;
    }
  }

  private static int columnIndex(MyTable table, MyColumn column) {
    int i = 0;
    for (MyColumn c: table.columns) {
      if (c == column) {
        return i;
      } else {
        ++i;
      }
    }
    throw new NoSuchElementException();
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

      LiveExpression[] template = new LiveExpression
        [tableReference.table.columns.size()];

      for (int i = 0; i < template.length; ++i) {
        template[i] = UndefinedExpression;
      }
      
      LiveExpression liveTest = test.makeLiveExpression(expressionContext);

      Iterator<MyColumn> columnIterator = columns.iterator();
      Iterator<MyExpression> valueIterator = values.iterator();
      while (columnIterator.hasNext()) {
        template[columnIndex(tableReference.table, columnIterator.next())]
          = valueIterator.next().makeLiveExpression(expressionContext);
      }

      MyTableReference.MySourceIterator iterator = tableReference.iterator
        (EmptyRevision, NullNodeStack, context.result, new NodeStack(),
         liveTest, expressionContext, false);

      List<MyColumn> keyColumns = tableReference.table.primaryKey.columns;
      LiveExpression[] key = new LiveExpression[keyColumns.size()];
      for (int i = 0; i < key.length; ++i) {
        key[i] = findColumnReference
          (expressionContext, tableReference, keyColumns.get(i));
      }

      context.setKey(0, tableReference.table);
      context.setKey(1, tableReference.table.primaryKey);

      Object[] tuple = new Object[template.length];
      int count = 0;

      while (true) {
        ResultType type = iterator.nextRow();
        switch (type) {
        case End:
          return count;
      
        case Inserted: {
          ++ count;

          Object[] original = (Object[]) iterator.pair.fork.value;
          for (int i = 0; i < tuple.length; ++i) {
            LiveExpression v = template[i];
            if (v == UndefinedExpression) {
              tuple[i] = original[i];
            } else {
              tuple[i] = v.evaluate(false);
            }
          }

          int i;
          for (i = 0; i < key.length - 1; ++i) {
            context.setKey
              (i + IndexBodyDepth, (Comparable) key[i].evaluate(false));
          }

          // todo: throw duplicate key exception if applicable
          context.insertOrUpdate
            (i + IndexBodyDepth, (Comparable) key[i].evaluate(false), tuple);
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
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

      MyTableReference.MySourceIterator iterator = tableReference.iterator
        (EmptyRevision, NullNodeStack, context.result, new NodeStack(),
         test.makeLiveExpression(expressionContext), expressionContext, false);

      List<MyColumn> keyColumns = tableReference.table.primaryKey.columns;
      LiveExpression[] key = new LiveExpression[keyColumns.size()];
      for (int i = 0; i < key.length; ++i) {
        key[i] = findColumnReference
          (expressionContext, tableReference, keyColumns.get(i));
      }

      context.setKey(0, tableReference.table);
      context.setKey(1, tableReference.table.primaryKey);

      int count = 0;

      while (true) {
        ResultType type = iterator.nextRow();
        switch (type) {
        case End:
          return count;
      
        case Inserted: {
          ++ count;

          int i;
          for (i = 0; i < key.length - 1; ++i) {
            context.setKey
              (i + IndexBodyDepth, (Comparable) key[i].evaluate(false));
          }

          context.delete
            (i + IndexBodyDepth, (Comparable) key[i].evaluate(false));
        } break;

        default:
          throw new RuntimeException("unexpected result type: " + type);
        }
      }
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

  public Column column(Class type) {
    return new MyColumn(type);
  }

  public Index index(List<Column> columns, boolean unique) {
    List copyOfColumns = new ArrayList(columns);

    for (Object c: copyOfColumns) {
      if (! (c instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    if (copyOfColumns.size() > MaxIndexDepth) {
      throw new IllegalArgumentException
        ("too many columns in index (maximum is " + MaxIndexDepth
         + "; got " + copyOfColumns.size() + ")");
    }

    return new MyIndex(copyOfColumns, unique);
  }

  public Table table(Set<Column> columns,
                     Index primaryKey,
                     Set<Index> indexes)
  {
    Collection copyOfColumns = new ArrayList(columns);

    for (Object c: copyOfColumns) {
      if (! (c instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }
    }

    MyIndex myPrimaryKey;
    try {
      myPrimaryKey = (MyIndex) primaryKey;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("index not created by this implementation");
    }

    if (! myPrimaryKey.unique) {
      throw new IllegalArgumentException("primary key must be unique");      
    }

    Collection copyOfIndexes = new ArrayList(indexes);

    for (Object i: copyOfIndexes) {
      if (! (i instanceof MyIndex)) {
        throw new IllegalArgumentException
          ("index not created by this implementation");
      }
    }

    copyOfIndexes.add(primaryKey);

    return new MyTable(Collections.unmodifiableCollection(copyOfColumns),
                       copyOfIndexes, myPrimaryKey);
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

    if (! myTableReference.table.columns.contains(myColumn)) {
      throw new IllegalArgumentException
        ("table does not contain specified column");
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

  public PatchTemplate insertTemplate(Table table,
                                      List<Column> columns,
                                      List<Expression> values,
                                      boolean updateOnDuplicateKey)
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

    Set set = new HashSet(myTable.columns);
    for (Object o: copyOfColumns) {
      if (! (o instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }

      if (! (set.contains(o))) {
        throw new IllegalArgumentException
          ("column not part of specified table");
      }

      set.remove(o);
    }

    if (set.size() != 0) {
      throw new IllegalArgumentException("not enough columns specified");
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
       updateOnDuplicateKey);
  }

  public PatchTemplate updateTemplate(TableReference tableReference,
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

    Set set = new HashSet(myTableReference.table.columns);
    for (Object o: copyOfColumns) {
      if (! (o instanceof MyColumn)) {
        throw new IllegalArgumentException
          ("column not created by this implementation");
      }

      if (! (set.contains(o))) {
        throw new IllegalArgumentException
          ("column not part of specified table");
      }

      set.remove(o);
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
  }

  public Revision commit(PatchContext context) {
    MyPatchContext myContext;
    try {
      myContext = (MyPatchContext) context;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException
        ("patch context not created by this implementation");        
    }

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

  private static Object[] makeTuple(MyTable table, Row row) {
    int i = 0;
    Object[] tuple = new Object[table.columns.size()];
    for (MyColumn c: table.columns) {
      tuple[i++] = row.value(c);
    }
    return tuple;
  }

  private static MyRevision mergeRevisions(MyRevision base,
                                           MyRevision left,
                                           MyRevision right,
                                           ConflictResolver conflictResolver)
  {
    MyPatchContext context = new MyPatchContext
      (new Object(), left, new NodeStack());

    MergeIterator[] iterators = new MergeIterator[MaxDepth + 1];
    
    NodeStack baseStack = new NodeStack();
    NodeStack leftStack = new NodeStack();
    NodeStack rightStack = new NodeStack();

    iterators[0] = new MergeIterator
      (base.root, baseStack, left.root, leftStack, right.root, rightStack);

    int depth = 0;
    int bottom = -1;
    MyTable table = null;
    MergeTriple triple = new MergeTriple();
    while (true) {
      if (iterators[depth].next(triple)) {
        boolean descend = false;
        boolean conflict = false;
        if (triple.base == null) {
          if (triple.left == null) {
            context.insertOrUpdate
              (depth, triple.right.key, triple.right.value);
          } else if (triple.right == null) {
            // do nothing -- left already has insert
          } else if (depth == bottom) {
            if (equal((Object[]) triple.left.value,
                      (Object[]) triple.right.value))
            {
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
              if (equal((Object[]) triple.left.value,
                        (Object[]) triple.right.value))
              {
                // do nothing -- updates match and left already has it
              } else {
                Object[] baseRow = (Object[]) triple.base.value;
                Object[] leftRow = (Object[]) triple.left.value;
                Object[] rightRow = (Object[]) triple.right.value;
                Object[] resultRow = new Object[baseRow.length];
                for (int i = 0; i < baseRow.length; ++i) {
                  if (equal(leftRow[i], rightRow[i])) {
                    resultRow[i] = leftRow[i];
                  } else if (equal(baseRow[i], leftRow[i])) {
                    resultRow[i] = rightRow[i];
                  } else if (equal(baseRow[i], rightRow[i])) {
                    resultRow[i] = leftRow[i];
                  } else {
                    conflict = true;
                  }
                }

                if (! conflict) {
                  context.insertOrUpdate(depth, triple.base.key, resultRow);
                }
              }
            } else {
              descend = true;
            }
          } else {
            context.delete(depth, triple.base.key);
          }
        } else {
          // do nothing -- left already has delete
        }

        if (conflict) {
          Row baseRow = triple.base == null
            ? null : new MyRow(table, (Object[]) triple.base.value);
          Row leftRow = new MyRow(table, (Object[]) triple.left.value);
          Row rightRow = new MyRow(table, (Object[]) triple.right.value);
          Row row = conflictResolver.resolveConflict
            (table, (Collection) table.columns, base, baseRow, left, leftRow,
             right, rightRow);
              
          if (row == leftRow) {
            // do nothing -- left already has insert
          } else if (row == rightRow) {
            context.insertOrUpdate
              (depth, triple.right.key, triple.right.value);
          } else {
            // todo: if the row is not equivalent with respect to the
            // primary key, either reject it or reset the key path in
            // the patch context so it is inserted in the right place
            context.insertOrUpdate
              (depth, triple.right.key, makeTuple(table, row));
          }
        } else if (descend) {
          context.setKey(depth, triple.left.key);

          if (depth == TableDepth) {
            table = (MyTable) triple.left.key;
          } else if (depth == IndexDepth) {
            bottom = ((MyIndex) triple.left.key).columns.size()
              + IndexBodyDepth - 1;
          }
          
          ++ depth;

          iterators[depth] = new MergeIterator
            ((Node) triple.base.value,
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

  private static void dump(Node node, java.io.PrintStream out, int depth) {
    if (node == NullNode) {
      return;
    } else {
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
      dump(node.left, out, depth + 1);
      dump(node.right, out, depth + 1);
    }
  }
}
