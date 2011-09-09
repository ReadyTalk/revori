package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.list;

import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Column;

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
      (Node.pathFind(base.root, Constants.IndexTable,
                     Constants.IndexTable.primaryKey, tableReference.table),
       baseStack = new NodeStack(baseStack),
       Node.pathFind(fork.root, Constants.IndexTable,
                     Constants.IndexTable.primaryKey, tableReference.table),
       forkStack = new NodeStack(forkStack),
       list(Interval.Unbounded).iterator(),
       true);

    boolean baseEmpty
      = Node.find(base.root, tableReference.table) == Node.Null;

    boolean forkEmpty
      = Node.find(fork.root, tableReference.table) == Node.Null;

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
