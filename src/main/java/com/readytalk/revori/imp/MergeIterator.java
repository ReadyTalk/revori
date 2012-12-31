/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.expect;

import java.util.Comparator;

class MergeIterator {
  public final NodeStack base;
  public final NodeStack left;
  public final NodeStack right;
  public final Comparator comparator;

  public MergeIterator(Node baseRoot,
                       NodeStack base,
                       Node leftRoot,
                       NodeStack left,
                       Node rightRoot,
                       NodeStack right,
                       Comparator comparator)
  {
    this.base = base;
    this.left = left;
    this.right = right;
    this.comparator = comparator;

    if (baseRoot != Node.Null) {
      base.push(baseRoot);
    }
    if (leftRoot != Node.Null) {
      left.push(leftRoot);
    }
    if (rightRoot != Node.Null) {
      right.push(rightRoot);
    }

    expect(base.top != Node.Null
           && left.top != Node.Null
           && right.top != Node.Null);

    // find leftmost nodes to start iteration
    while (true) {
      int leftBase = compareForDescent(left.top, base.top, comparator);
      if (leftBase > 0) {
        int rightBase = compareForDescent(right.top, base.top, comparator);
        if (rightBase > 0) {
          int leftRight = compareForDescent(left.top, right.top, comparator);
          if (leftRight > 0) {
            // base < right < left
            left.descendLeft();
          } else if (leftRight < 0) {
            // base < left < right
            right.descendLeft();
          } else {
            // base < left = right
            if (left.top.left == Node.Null && right.top.left == Node.Null) {
              while (base.top.left != Node.Null) {
                base.push(base.top.left);
              }
              break;
            } else {
              left.descendLeft();
              right.descendLeft();
            }
          }
        } else {
          // right <= base < left
          left.descendLeft();
        }
      } else if (leftBase < 0) {
        int rightBase = compareForDescent(right.top, base.top, comparator);
        if (rightBase > 0) {
          // left < base < right
          right.descendLeft();
        } else if (rightBase < 0) {
          // left/right < base
          base.descendLeft();
        } else {
          // left < right = base

          // todo: if right.top == base.top and we're basing the
          // merge result on left, we can break here instead of
          // descending further

          if (right.top.left == Node.Null && base.top.left == Node.Null) {
            while (left.top.left != Node.Null) {
              left.push(left.top.left);
            }
            break;
          } else {
            right.descendLeft();
            base.descendLeft();
          }
        }
      } else {
        int rightBase = compareForDescent(right.top, base.top, comparator);
        if (rightBase > 0) {
          // left = base < right
          right.descendLeft();
        } else if (rightBase < 0) {
          // right < left = base
          if (left.top.left == Node.Null && base.top.left == Node.Null) {
            while (right.top.left != Node.Null) {
              right.push(right.top.left);
            }
            break;
          } else {
            left.descendLeft();
            right.descendLeft();
          }
        } else {
          // right = left = base
          if (left.top == right.top && left.top == base.top) {
            // no need to go any deeper -- there aren't any changes
            break;
          } else if ((left.top == null || left.top.left == Node.Null)
                     && (right.top == null || right.top.left == Node.Null)
                     && (base.top == null || base.top.left == Node.Null))
          {
            break;
          } else {
            left.descendLeft();
            right.descendLeft();
            base.descendLeft();
          }
        }
      }
    }

    expect(base.top != Node.Null
           && left.top != Node.Null
           && right.top != Node.Null);
  }

  private static int compareForMerge(Node a, Node b, Comparator comparator) {
    if (a == null || b == null) {
      return 0;
    }

    return Compare.compare(a.key, b.key, comparator);
  }

  private static int compareForDescent(Node a, Node b, Comparator comparator) {
    if (a == null || b == null) {
      return 0;
    }

    int difference = Compare.compare(a.key, b.key, comparator);
    if (difference > 0) {
      if (a.left == Node.Null) {
        return 0;
      } else {
        return 1;
      }
    } else if (difference < 0) {
      if (b.left == Node.Null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      return 0;
    }
  }

  public boolean next(MergeTriple triple) {
    while (true) {
      int leftBase = compareForMerge(left.top, base.top, comparator);
      if (leftBase > 0) {
        int rightBase = compareForMerge(right.top, base.top, comparator);
        if (rightBase > 0) {
          // base < right/left
          triple.left = null;
          triple.right = null;
          triple.base = base.top;
          base.next();
        } else if (rightBase < 0) {
          // right < base < left
          triple.left = null;
          triple.right = right.top;
          triple.base = null;
          right.next();
        } else {
          // right = base < left
          if (right.top == null && base.top == null) {
            triple.left = left.top;
            triple.right = null;
            triple.base = null;
            left.next();
          } else {
            // todo: if right.top == base.top and we're basing the
            // merge result on left, we can skip this part of the
            // tree

            triple.left = null;
            triple.right = right.top;
            triple.base = base.top;
            right.next();
            base.next();
          }
        }
      } else if (leftBase < 0) {
        int rightBase = compareForMerge(right.top, base.top, comparator);
        if (rightBase >= 0) {
          // left < right <= base
          triple.left = left.top;
          triple.right = null;
          triple.base = null;
          left.next();
        } else {
          int leftRight = compareForMerge(left.top, right.top, comparator);
          if (leftRight > 0) {
            // right < left < base
            triple.left = null;
            triple.right = right.top;
            triple.base = null;
            right.next();
          } else if (leftRight < 0) {
            // left < right < base
            triple.left = left.top;
            triple.right = null;
            triple.base = null;
            left.next();
          } else {
            // left = right < base
            if (left.top == null && right.top == null) {
              triple.left = null;
              triple.right = null;
              triple.base = base.top;
              base.next();
            } else {
              triple.left = left.top;
              triple.right = right.top;
              triple.base = null;
              left.next();
              right.next();
            }
          }
        }
      } else {
        int rightBase = compareForMerge(right.top, base.top, comparator);
        if (rightBase > 0) {
          // left = base < right
          if (left.top == null && base.top == null) {
            triple.left = null;
            triple.right = right.top;
            triple.base = null;
            base.next();
          } else {
            triple.left = left.top;
            triple.right = null;
            triple.base = base.top;
            left.next();
            base.next();
          }
        } else if (rightBase < 0) {
          // right < left = base
          triple.left = null;
          triple.right = right.top;
          triple.base = null;
          right.next();
        } else if (base.top == null) {
          int leftRight = compareForMerge(left.top, right.top, comparator);
          if (leftRight > 0) {
            // right < left
            triple.left = null;
            triple.right = right.top;
            triple.base = null;
            right.next();
          } else if (leftRight < 0) {
            // left < right
            triple.left = left.top;
            triple.right = null;
            triple.base = null;
            left.next();
          } else {
            // left = right
            if (left.top == null && right.top == null) {
              return false;
            } else {
              triple.left = left.top;
              triple.right = right.top;
              triple.base = null;
              left.next();
              right.next();
            }
          }
        } else {
          // left = right = base
          if (left.top == right.top && left.top == base.top) {
            // no need to go any deeper -- there aren't any changes
            left.ascendNext();
            right.ascendNext();
            base.ascendNext();
            continue;
          } else {
            triple.left = left.top;
            triple.right = right.top;
            triple.base = base.top;
            left.next();
            right.next();
            base.next();
          }
        }
      }
      return true;
    }
  }

  public static class MergeTriple {
    public Node base;
    public Node left;
    public Node right;
  }
}
