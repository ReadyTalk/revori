/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.readytalk.revori.QueryResult;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.imp.DiffIterator.DiffPair;

class TableIterator implements SourceIterator {
  private static final Logger log = LoggerFactory.getLogger(TableIterator.class);

  private final TableReference tableReference;
  private final Node base;
  private final Node fork;
  public final ExpressionAdapter test;
  private final ExpressionContext expressionContext;
  private final boolean visitUnchanged;
  private final List<ColumnReferenceAdapter> columnReferences = new ArrayList<ColumnReferenceAdapter>();
  private final Plan plan;
  public final DiffPair pair = new DiffPair();
  private NodeStack baseStack;
  private NodeStack forkStack;
  private int depth;
  public boolean testFork;

  public TableIterator(TableReference tableReference,
                       DefaultRevision base,
                       NodeStack baseStack,
                       DefaultRevision fork,
                       NodeStack forkStack,
                       ExpressionAdapter test,
                       ExpressionContext expressionContext,
                       Plan plan,
                       boolean visitUnchanged)
  {
    this.tableReference = tableReference;
    this.base = Node.pathFind
      (base.root, tableReference.table, Compare.TableComparator);
    this.fork = Node.pathFind
      (fork.root, tableReference.table, Compare.TableComparator);
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
      (Node.pathFind(this.base, plan.index, Compare.IndexComparator),
       this.baseStack = new NodeStack(baseStack),
       Node.pathFind(this.fork, plan.index, Compare.IndexComparator),
       this.forkStack = new NodeStack(forkStack),
       plan.scans[0].evaluate().iterator(),
       visitUnchanged, plan.index.columns.get(0).comparator);

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
                       DefaultRevision base,
                       NodeStack baseStack,
                       DefaultRevision fork,
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
      log.trace("depth: {}\n\tkey: {}", depth, (Constants.IndexDataBodyDepth + tableReference.table.primaryKey.columns.size()));
      if (plan.iterators[depth].next(pair)) {
        if (depth == plan.size - 1) {
          if (test(pair.base)) {
            if (pair.fork == null) {
              return QueryResult.Type.Deleted;
            } else if (pair.base == pair.fork
                       || (expressionContext.queryExpressions == null
                           ? Node.treeEqual(baseStack, (Node) pair.base.value,
                                            forkStack, (Node) pair.fork.value,
                                            plan.iterators[depth].comparator)
                           : valuesEqual(expressionContext.queryExpressions,
                                         expressionContext.columnReferences,
                                         (Node) pair.base.value,
                                         (Node) pair.fork.value)))
                       
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
    // TODO: this is a really ugly way to avoid affecting the expression evaluation results
    // The real problem is that test(pair.fork) is side-affecting - it causes
    //   column references to be set to the fork values, where they might have
    //   been the base values (for example, if rowUpdated is called right after getting
    //   a QueryResult.Type.Deleted).  The real fix should involve removing
    //   the side-affecting nature of test().
    Object[] values = new Object[columnReferences.size()];
    { int i = 0;
      for (ColumnReferenceAdapter r: columnReferences) {
        values[i++] = r.value;
      }
    }

    boolean v = depth == plan.size - 1
      && pair.base != null
      && pair.fork != null
      && testFork
      && test(pair.fork);

    { int i = 0;
      for (ColumnReferenceAdapter r: columnReferences) {
        r.value = values[i++];
      }
    }

    return v;
  }

  private static void setValue(ColumnReferenceAdapter r, Node tree) {
    Object v = Node.find(tree, r.column, Compare.ColumnComparator).value();
    if (v != null && ! r.column.type.isInstance(v)) {
      throw new ClassCastException
        (v.getClass().getName() + " cannot be cast to "
         + r.column.type.getName());
    }
    r.value = v;
  }

  private static Object[] evaluate
    (List<ExpressionAdapter> expressions,
     Set<ColumnReferenceAdapter> columnReferences, Node tree)
  {
    for (ColumnReferenceAdapter r: columnReferences) {
      setValue(r, tree);
    }

    Object[] values = new Object[expressions.size()];
    int i = 0;
    for (ExpressionAdapter e: expressions) {
      values[i++] = e.evaluate(false);
    }

    return values;
  }

  private static boolean valuesEqual
    (List<ExpressionAdapter> expressions,
     Set<ColumnReferenceAdapter> columnReferences, Node base, Node fork)
  {
    Object[] forkValues = evaluate(expressions, columnReferences, fork);
    Object[] baseValues = evaluate(expressions, columnReferences, base);

    boolean equal = Arrays.equals(baseValues, forkValues);
    log.trace("valuesEqual: {}\n\tbase: {}\n\tfork: {}", equal, Arrays.toString(baseValues), Arrays.toString(forkValues));
    //    System.exit(0);
    return equal;
  }

  private boolean test(Node node) {
    if (node != null) {
      Node tree = (Node) node.value;
        
      for (ColumnReferenceAdapter r: columnReferences) {
        setValue(r, tree);
      }

      Object result = test.evaluate(false);

      if (log.isDebugEnabled()) {
        StringBuilder test = new StringBuilder("test: ");
        for (ColumnReferenceAdapter r: expressionContext.columnReferences) {
          test.append(r.column).append(":").append(r.value).append(" ");
        }
        test.append(": ").append(result);
        log.debug(test.toString());
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
       visitUnchanged, depth == plan.index.columns.size()
       ? Compare.ColumnComparator : plan.index.columns.get(depth).comparator);
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
