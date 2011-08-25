package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.imp.DiffIterator.DiffPair;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.TableReference;

import java.util.List;
import java.util.ArrayList;

class TableIterator implements SourceIterator {
  private static final boolean Verbose = false;

  public final TableReference tableReference;
  public final Node base;
  public final Node fork;
  public final ExpressionAdapter test;
  public final ExpressionContext expressionContext;
  public final boolean visitUnchanged;
  public final List<ColumnReferenceAdapter> columnReferences = new ArrayList<ColumnReferenceAdapter>();
  public final Plan plan;
  public final DiffPair pair = new DiffPair();
  public NodeStack baseStack;
  public NodeStack forkStack;
  public int depth;
  public boolean testFork;

  public TableIterator(TableReference tableReference,
                       MyRevision base,
                       NodeStack baseStack,
                       MyRevision fork,
                       NodeStack forkStack,
                       ExpressionAdapter test,
                       ExpressionContext expressionContext,
                       Plan plan,
                       boolean visitUnchanged)
  {
    this.tableReference = tableReference;
    this.base = Node.pathFind(base.root, tableReference.table);
    this.fork = Node.pathFind(fork.root, tableReference.table);
    this.test = test;
    this.expressionContext = expressionContext;
    this.visitUnchanged = visitUnchanged;
    this.plan = plan;

    // System.out.println("base:");
    // Node.dump(this.base, System.out, 1);
    // System.out.println("fork:");
    // Node.dump(this.fork, System.out, 1);
    // System.out.println("intervals: " + plan.scans[0].evaluate());

    plan.iterators[0] = new DiffIterator
      (Node.pathFind(this.base, plan.index),
       this.baseStack = new NodeStack(baseStack),
       Node.pathFind(this.fork, plan.index),
       this.forkStack = new NodeStack(forkStack),
       plan.scans[0].evaluate().iterator(),
       visitUnchanged);

    for (ColumnReferenceAdapter r: expressionContext.columnReferences) {
      if (r.tableReference == tableReference) {
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

  public TableIterator(TableReference tableReference,
                       MyRevision base,
                       NodeStack baseStack,
                       MyRevision fork,
                       NodeStack forkStack,
                       ExpressionAdapter test,
                       ExpressionContext expressionContext,
                       boolean visitUnchanged)
  {
    this(tableReference, base, baseStack, fork, forkStack, test,
         expressionContext, Plan.choosePlan
         (base, baseStack, fork, forkStack, test, tableReference),
         visitUnchanged);
  }

  public QueryResult.Type nextRow() {
    if (testFork) {
      testFork = false;
      if (test(pair.fork)) {
        return QueryResult.Type.Inserted;
      }
    }

    while (true) {
      if (plan.iterators[depth].next(pair)) {
        if (depth == plan.size - 1) {
          if (test(pair.base)) {
            if (pair.fork == null) {
              return QueryResult.Type.Deleted;
            } else if (pair.base == pair.fork
                       || Node.treeEqual(baseStack, (Node) pair.base.value,
                                         forkStack, (Node) pair.fork.value))
            {
              if (visitUnchanged) {
                return QueryResult.Type.Unchanged;
              }
            } else {
              testFork = true;
              return QueryResult.Type.Deleted;                  
            }
          } else if (test(pair.fork)) {
            return QueryResult.Type.Inserted;
          }
        } else {
          descend(pair);
        }
      } else if (depth == 0) {
        // todo: be defensive to ensure we can safely keep
        // returning QueryResult.Type.End if the application calls
        // nextRow again after this.  The popStack calls below
        // should not be called more than once.

        for (ColumnReferenceAdapter r: columnReferences) {
          r.value = Compare.Undefined;
        }

        baseStack.popStack();
        forkStack.popStack();
        
        baseStack = null; // for safety
        forkStack = null; // ditto

        return QueryResult.Type.End;
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
        
      for (ColumnReferenceAdapter r: columnReferences) {
        Object v = Node.find(tree, r.column).value();
        if (v != null && ! r.column.type.isInstance(v)) {
          throw new ClassCastException
            (v.getClass().getName() + " cannot be cast to "
             + r.column.type.getName());
        }
        r.value = v;
      }

      Object result = test.evaluate(false);

      if (Verbose) {
        System.out.print("test: ");
        for (ColumnReferenceAdapter r: expressionContext.columnReferences) {
          System.out.print(r.column + ":" + r.value + " ");
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
        
    ColumnReferenceAdapter reference = plan.references[depth];
    if (reference != null) {
      reference.value = base == null ? fork.key : base.key;
    }

    ++ depth;

    plan.iterators[depth] = new DiffIterator
      (base == null ? Node.Null : (Node) base.value,
       baseStack = new NodeStack(baseStack),
       fork == null ? Node.Null : (Node) fork.value,
       forkStack = new NodeStack(forkStack),
       plan.scans[depth].evaluate().iterator(),
       visitUnchanged);
  }

  private void ascend() {
    plan.iterators[depth] = null;

    -- depth;

    ColumnReferenceAdapter reference = plan.references[depth];
    if (reference != null) {
      reference.value = Compare.Undefined;
    }

    baseStack = baseStack.popStack();
    forkStack = forkStack.popStack();
  }
}
