/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.google.common.collect.Lists;
import com.readytalk.revori.Column;
import com.readytalk.revori.Index;
import com.readytalk.revori.TableReference;

class Plan {
  public final Index index;
  public final int size;
  public final ColumnReferenceAdapter[] references;
  public final Scan[] scans;
  public final DiffIterator[] iterators;
  public boolean match;
  public boolean complete = true;

  public Plan(Index index) {
    this.index = index;
    this.size = index.columns.size();
    this.references = new ColumnReferenceAdapter[size];
    this.scans = new Scan[size];
    this.iterators = new DiffIterator[size];
  }

  private static ColumnReferenceAdapter findColumnReference
    (ExpressionAdapter expression,
     TableReference tableReference,
     Column<?> column)
  {
    ColumnReferenceFinder finder = new ColumnReferenceFinder
      (tableReference, column);
    expression.visit(finder);
    return finder.reference;
  }

  private static Plan improvePlan(Plan best,
                                  Index index,
                                  ExpressionAdapter test,
                                  TableReference tableReference)
  {
    Plan plan = new Plan(index);

    for (int i = 0; i < plan.size; ++i) {
      Column<?> column = index.columns.get(i);

      ColumnReferenceAdapter reference = findColumnReference
        (test, tableReference, column);

      boolean match = false;

      if (reference != null) {
        Scan scan = test.makeScan(reference);

        plan.scans[i] = scan;

        if (scan.isUseful()) {
          plan.match = true;
          match = true;
        }

        reference.value = Compare.Dummy;
        plan.references[i] = reference;
      } else {
        plan.scans[i] = IntervalScan.Unbounded;
      }

      if (! match) {
        plan.complete = false;
      }
    }

    for (int i = 0; i < plan.size; ++i) {
      ColumnReferenceAdapter reference = plan.references[i];
      if (reference != null) {
        reference.value = Compare.Undefined;
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

  public static Plan choosePlan(MyRevision base,
                                NodeStack baseStack,
                                MyRevision fork,
                                NodeStack forkStack,
                                ExpressionAdapter test,
                                TableReference tableReference)
  {
    Plan best = improvePlan
      (null, tableReference.table.primaryKey, test, tableReference);

    DiffIterator indexIterator = new DiffIterator
      (Node.pathFind(base.root, Constants.IndexTable, Compare.TableComparator,
                     Constants.IndexTable.primaryKey, Compare.IndexComparator,
                     tableReference.table, Constants.TableColumn.comparator),
       baseStack = new NodeStack(baseStack),
       Node.pathFind(fork.root, Constants.IndexTable, Compare.TableComparator,
                     Constants.IndexTable.primaryKey, Compare.IndexComparator,
                     tableReference.table, Constants.TableColumn.comparator),
       forkStack = new NodeStack(forkStack),
       Lists.newArrayList(Interval.Unbounded).iterator(),
       true, Compare.IndexComparator);

    boolean baseEmpty
      = Node.find(base.root, tableReference.table, Compare.TableComparator)
      == Node.Null;

    boolean forkEmpty
      = Node.find(fork.root, tableReference.table, Compare.TableComparator)
      == Node.Null;

    DiffIterator.DiffPair pair = new DiffIterator.DiffPair();
    while (indexIterator.next(pair)) {
      if ((pair.base == null && ! baseEmpty)
          || (pair.fork == null && ! forkEmpty))
      {
        continue;
      }

      Index index = (Index)
        (pair.base == null ? pair.fork.key : pair.base.key);

      if (! index.equals(tableReference.table.primaryKey)) {
        best = improvePlan(best, index, test, tableReference);
      }
    }

    baseStack.popStack();
    forkStack.popStack();

    return best;
  }

  private static class ColumnReferenceFinder
    implements ExpressionAdapterVisitor
  {
    public final TableReference tableReference;
    public final Column<?> column;
    public ColumnReferenceAdapter reference;

    public ColumnReferenceFinder(TableReference tableReference,
                                 Column<?> column)
    {
      this.tableReference = tableReference;
      this.column = column;
    }

    public void visit(ExpressionAdapter e) {
      if (e instanceof ColumnReferenceAdapter) {
        ColumnReferenceAdapter r = (ColumnReferenceAdapter) e;
        if (r.tableReference == tableReference
            && r.column == column)
        {
          reference = r;
        }
      }
    }
  }
}
