/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.expect;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.readytalk.revori.Column;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.Index;
import com.readytalk.revori.Table;
import com.readytalk.revori.View;

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

    if (base.equals(right) || left.equals(right)) {
      return left;
    } else if (base.equals(left)) {
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
    //     obsolete index and view data trees.
    //
    //  2. Update non-primary-key index and view data trees, removing
    //     obsolete rows and adding new or updated ones.
    //
    //  3. Build data trees for any new indexes and views added.
    //
    //  4. Verify foreign key constraints.

    MyRevisionBuilder builder = new MyRevisionBuilder
      (new Object(), left, new NodeStack());

    Set<Index> indexes = new TreeSet<Index>();
    Set<Index> newIndexes = new TreeSet<Index>();

    Set<View> views = new TreeSet<View>();
    Set<View> newViews = new TreeSet<View>();

    NodeStack baseStack = new NodeStack();
    NodeStack leftStack = new NodeStack();
    NodeStack rightStack = new NodeStack();

    { MergeIterator[] iterators = new MergeIterator[Constants.MaxDepth + 1];
      iterators[0] = new MergeIterator
        (base.root, baseStack, left.root, leftStack, right.root, rightStack,
         Compare.TableComparator);

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

          Comparator comparator = iterators[depth].comparator;

          boolean descend = false;
          boolean conflict = false;
          if (triple.base == null) {
            if (triple.left == null) {
              if(depth != Constants.IndexDataDepth || ((Index)triple.right.key).isPrimary()) {
                builder.insertOrUpdate
                  (depth, triple.right.key, comparator,
                   triple.right.value);
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
                  (depth, triple.right.key, comparator,
                   triple.right.value);
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
                    (depth, triple.right.key, comparator,
                     triple.right.value);
                } else {
                  conflict = true;
                }
              } else {
                descend = true;
              }
            } else if (depth != bottom) {
              descend = true;
            } else {
              builder.deleteKey(depth, triple.left.key, comparator);
            }
          } else if (depth != bottom) {
            descend = true;
          } else {
            // do nothing -- left already has delete
          }

          Object key = triple.base == null
            ? (triple.left == null
               ? triple.right.key
               : triple.left.key)
            : triple.base.key;

          if (conflict) {
            Object[] primaryKeyValues = new Object
              [depth - Constants.IndexDataBodyDepth];

            for (int i = 0; i < primaryKeyValues.length; ++i) {
              primaryKeyValues[i] = builder.keys
                [i + Constants.IndexDataBodyDepth];
            }

            Object result = conflictResolver.resolveConflict
              (table,
               (Column<?>) key,
               primaryKeyValues,
               triple.base == null ? null : triple.base.value,
               triple.left.value,
               triple.right.value);

            if (Compare.equal(result, triple.left.value)) {
              // do nothing -- left already has insert
            } else if (result == null) {
              builder.deleteKey(depth, key, comparator);
            } else {
              builder.insertOrUpdate(depth, key, comparator, result);
            }
          } else if (descend) {
            Comparator nextComparator;
            if (depth == Constants.TableDataDepth) {
              table = (Table) key;

              if (Node.pathFind
                  (left.root, Constants.ViewTable, Compare.TableComparator,
                   Constants.ViewTableIndex, Compare.IndexComparator,
                   table, Constants.ViewTableColumn.comparator) != Node.Null)
              {
                // skip views -- we'll handle them later
                continue;
              }

              nextComparator = Compare.IndexComparator;

              { DiffIterator indexIterator = new DiffIterator
                  (Node.pathFind
                   (left.root, Constants.IndexTable, Compare.TableComparator,
                    Constants.IndexTable.primaryKey, Compare.IndexComparator,
                    table, Constants.TableColumn.comparator),
                   baseStack = new NodeStack(baseStack),
                   Node.pathFind
                   (builder.result.root,
                    Constants.IndexTable, Compare.TableComparator,
                    Constants.IndexTable.primaryKey, Compare.IndexComparator,
                    table, Constants.TableColumn.comparator),
                   leftStack = new NodeStack(leftStack),
                   Lists.newArrayList(Interval.Unbounded).iterator(),
                   true, Constants.IndexColumn.comparator);
          
                DiffIterator.DiffPair pair = new DiffIterator.DiffPair();
                while (indexIterator.next(pair)) {
                  if (pair.base != null) {
                    if (pair.fork != null) {
                      Index index = (Index) pair.base.key;
                      if (! index.equals(table.primaryKey)) {
                        indexes.add(index);
                      }
                    } else {
                      builder.setKey
                        (Constants.TableDataDepth, table,
                         Compare.TableComparator);
                      builder.deleteKey
                        (Constants.IndexDataDepth, pair.base.key,
                         Compare.IndexComparator);
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

              { DiffIterator indexIterator = new DiffIterator
                  (Node.pathFind
                   (left.root, Constants.ViewTable, Compare.TableComparator,
                    Constants.ViewTable.primaryKey, Compare.IndexComparator,
                    table, Constants.TableColumn.comparator),
                   baseStack = new NodeStack(baseStack),
                   Node.pathFind
                   (builder.result.root,
                    Constants.ViewTable, Compare.TableComparator,
                    Constants.ViewTable.primaryKey, Compare.IndexComparator,
                    table, Constants.TableColumn.comparator),
                   leftStack = new NodeStack(leftStack),
                   Lists.newArrayList(Interval.Unbounded).iterator(),
                   true, Constants.ViewColumn.comparator);
          
                DiffIterator.DiffPair pair = new DiffIterator.DiffPair();
                while (indexIterator.next(pair)) {
                  if (pair.base != null) {
                    View view = (View) pair.base.key;
                    if (pair.fork != null) {
                      views.add(view);
                    } else {
                      builder.deleteKey
                        (Constants.TableDataDepth, view.table,
                         Compare.TableComparator);
                    }
                  } else if (pair.fork != null) {
                    newViews.add((View) pair.fork.key);
                  }
                }

                baseStack = baseStack.popStack();
                leftStack = leftStack.popStack();
              }
            } else if (depth == Constants.IndexDataDepth) {
              Index index = (Index) key;

              if (Compare.equal(index, table.primaryKey, comparator)) {
                bottom = index.columns.size() + Constants.IndexDataBodyDepth;
              } else {
                // skip non-primary-key index data trees -- we'll handle
                // those later
                continue;
              }

              nextComparator = table.primaryKey.columns.get(0).comparator;
            } else if (depth + 1 == bottom) {
              nextComparator = Compare.ColumnComparator;
            } else {
              nextComparator = table.primaryKey.columns.get
                (depth + 1 - Constants.IndexDataBodyDepth).comparator;
            }

            builder.setKey(depth, key, comparator);
          
            ++ depth;

            iterators[depth] = new MergeIterator
              (triple.base == null ? Node.Null : (Node) triple.base.value,
               baseStack = new NodeStack(baseStack),
               triple.left == null ? Node.Null : (Node) triple.left.value,
               leftStack = new NodeStack(leftStack),
               triple.right == null ? Node.Null : (Node) triple.right.value,
               rightStack = new NodeStack(rightStack),
               nextComparator);
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

    // Update non-primary-key index data trees
    for (Index index: indexes) {
      builder.updateIndexTree(index, left, leftStack, baseStack);
    }

    // Update view data trees
    for (View view: views) {
      builder.updateViewTree(view, left, leftStack, baseStack);
    }

    // build data trees for any new indexes
    for (Index index: newIndexes) {
      builder.updateIndexTree(index, MyRevision.Empty, leftStack, baseStack);
    }

    // build data trees for any new views
    for (View view: newViews) {
      builder.updateViewTree(view, MyRevision.Empty, leftStack, baseStack);
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
