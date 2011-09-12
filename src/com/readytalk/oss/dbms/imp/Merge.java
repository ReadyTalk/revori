package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.expect;
import static com.readytalk.oss.dbms.util.Util.list;

import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.ForeignKeyResolver;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.DiffResult;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

class Merge {
  public static MyRevision mergeRevisions
    (MyRevision base,
     MyRevision left,
     MyRevision right,
     ConflictResolver conflictResolver,
     ForeignKeyResolver foreignKeyResolver)
  {
    // System.out.println("base:");
    // Node.dump(base.root, System.out, 0);
    // System.out.println("left:");
    // Node.dump(left.root, System.out, 0);
    // System.out.println("right:");
    // Node.dump(right.root, System.out, 0);

    if (base.root == right.root
        || left.root == right.root)
    {
      return left;
    } else if (base.root == left.root) {
      if (left.equals(right)) {
        return left;
      } else {
        return right;
      }
    }
 
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
    //
    //  4. Verify foreign keys constraints.

    MyRevisionBuilder builder = new MyRevisionBuilder
      (new Object(), left, new NodeStack());

    Set<Index> indexes = new TreeSet<Index>();
    Set<Index> newIndexes = new TreeSet<Index>();

    NodeStack baseStack = new NodeStack();
    NodeStack leftStack = new NodeStack();
    NodeStack rightStack = new NodeStack();

    { MergeIterator[] iterators = new MergeIterator[Constants.MaxDepth + 1];
    
      iterators[0] = new MergeIterator
        (base.root, baseStack, left.root, leftStack, right.root, rightStack);

      int depth = 0;
      int bottom = -1;
      Table table = null;
      MergeIterator.MergeTriple triple = new MergeIterator.MergeTriple();

      // merge primary key data trees of each table, deleting obsolete
      // rows from any other index data trees as we go
      while (true) {
        if (iterators[depth].next(triple)) {
          expect(triple.base != Node.Null
                 && triple.left != Node.Null
                 && triple.right != Node.Null);

          boolean descend = false;
          boolean conflict = false;
          if (triple.base == null) {
            if (triple.left == null) {
              if(depth != Constants.IndexDataDepth || ((Index)triple.right.key).isPrimary()) {
                builder.insertOrUpdate
                  (depth, triple.right.key, triple.right.value);
              }
            } else if (triple.right == null) {
              // do nothing -- left already has insert
            } else if (depth == bottom) {
              if (Compare.equal(triple.left.value, triple.right.value)) {
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
                if(depth == Constants.IndexDataDepth) {
                  indexes.remove(triple.right.key);
                }
                builder.insertOrUpdate
                  (depth, triple.right.key, triple.right.value);
              } else if (triple.right == triple.base) {
                // do nothing -- left already has update
              } else if (depth == bottom) {
                if (Compare.equal(triple.left.value, triple.right.value)
                    || Compare.equal(triple.base.value, triple.right.value))
                {
                  // do nothing -- updates match or only left changed,
                  // and left already has it
                } else if (Compare.equal(triple.base.value, triple.left.value))
                {
                  builder.insertOrUpdate
                    (depth, triple.right.key, triple.right.value);
                } else {
                  conflict = true;
                }
              } else {
                descend = true;
              }
            } else {
              builder.delete(depth, triple.left.key);
            }
          } else {
            // do nothing -- left already has delete
          }

          if (conflict) {
            Object[] primaryKeyValues = new Object
              [depth - Constants.IndexDataBodyDepth];

            for (int i = 0; i < primaryKeyValues.length; ++i) {
              primaryKeyValues[i] = builder.keys
                [i + Constants.IndexDataBodyDepth];
            }

            Object result = conflictResolver.resolveConflict
              (table,
               (Column<?>) triple.left.key,
               primaryKeyValues,
               triple.base == null ? null : triple.base.value,
               triple.left.value,
               triple.right.value);

            if (Compare.equal(result, triple.left.value)) {
              // do nothing -- left already has insert
            } else if (result == null) {
              builder.delete(depth, triple.left.key);
            } else {
              builder.insertOrUpdate(depth, triple.left.key, result);
            }
          } else if (descend) {
            if (depth == Constants.TableDataDepth) {
              table = (Table) triple.left.key;

              if (table != Constants.IndexTable) {
                DiffIterator indexIterator = new DiffIterator
                  (Node.pathFind(left.root, Constants.IndexTable,
                                 Constants.IndexTable.primaryKey, table),
                   baseStack = new NodeStack(baseStack),
                   Node.pathFind(builder.result.root, Constants.IndexTable,
                                 Constants.IndexTable.primaryKey, table),
                   leftStack = new NodeStack(leftStack),
                   list(Interval.Unbounded).iterator(),
                   true);
          
                DiffIterator.DiffPair pair = new DiffIterator.DiffPair();
                while (indexIterator.next(pair)) {
                  if (pair.base != null) {
                    if (pair.fork != null) {
                      Index index = (Index) pair.base.key;
                      if (! index.equals(table.primaryKey)) {
                        indexes.add(index);
                      }
                    } else {
                      builder.setKey(Constants.TableDataDepth, table);
                      builder.delete
                        (Constants.IndexDataDepth, (Index) pair.base.key);
                    }
                  } else if (pair.fork != null) {
                    Index index = (Index) pair.fork.key;
                    if (! index.equals(table.primaryKey)) {
                      newIndexes.add(index);
                    }
                  }
                }

                baseStack = baseStack.popStack();
                leftStack = leftStack.popStack();
              }
            } else if (depth == Constants.IndexDataDepth) {
              Index index = (Index) triple.left.key;
              if (Compare.equal(index, table.primaryKey)) {
                bottom = index.columns.size() + Constants.IndexDataBodyDepth;
              } else {
                // skip non-primary-key index data trees -- we'll handle
                // those later
                continue;
              }
            }

            builder.setKey(depth, triple.left.key);
          
            ++ depth;

            iterators[depth] = new MergeIterator
              (triple.base == null ? Node.Null : (Node) triple.base.value,
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

          baseStack = baseStack.popStack();
          leftStack = leftStack.popStack();
          rightStack = rightStack.popStack();
        }
      }
    }

    // Update non-primary-key data trees
    for (Index index: indexes) {
      builder.updateIndexTree(index, left, leftStack, baseStack);
    }

    // build data trees for any new index keys
    for (Index index: newIndexes) {
      builder.updateIndexTree(index, MyRevision.Empty, leftStack, baseStack);
    }

    // verify all foreign key constraints
    ForeignKeys.checkForeignKeys
      (leftStack, left, baseStack, builder, rightStack, foreignKeyResolver,
       null);

    ForeignKeys.checkForeignKeys
      (rightStack, right, baseStack, builder, leftStack, foreignKeyResolver,
       null);

    // System.out.println("merge base");
    // Node.dump(base.root, System.out, 1);
    // System.out.println("merge left");
    // Node.dump(left.root, System.out, 1);
    // System.out.println("merge right");
    // Node.dump(right.root, System.out, 1);
    // System.out.println("merge result");
    // Node.dump(builder.result.root, System.out, 1);
    // System.out.println();

    if (left.equals(builder.result)) {
      return left;
    } else if (base.equals(builder.result)) {
      return base;
    } else {
      return builder.result;
    }
  }
}
