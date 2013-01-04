/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import static com.readytalk.revori.util.Util.expect;

import java.util.Comparator;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

class Node {
  private static final boolean Debug = false;

  public static final Node Null = new Node(new Object(), null);

  static {
    Null.left = Null;
    Null.right = Null;
    Null.value = Null;
  }

  public final Object token;
  public Object key;
  public Object value;
  public Node left;
  public Node right;
  public boolean red;
    
  public Node(Object token, @Nullable Node basis) {
    this.token = token;

    if (basis != null) {
      key = basis.key;
      value = basis.value;
      left = basis.left;
      right = basis.right;
      red = basis.red;
    }
  }

  public static Node getNode(Object token, Node basis) {
    if (basis.token == token) {
      return basis;
    } else {
      return new Node(token, basis);
    }
  }

  public static Node pathFind(Node root, Object ... path) {
    for (int i = 0; i < path.length && root != Null; ++i) {
      root = (Node) find(root, path[i], (Comparator) path[++i]).value;
    }
    return root;
  }

  public static Node find(Node n, Object key, Comparator comparator) {
    while (n != Null) {
      int difference = Compare.compare(key, n.key, comparator);
      if (difference < 0) {
        n = n.left;
      } else if (difference > 0) {
        n = n.right;
      } else {
        return n;
      }
    }
    return Null;
  }

  private static Node leftRotate(Object token, Node n) {
    if (Debug) expect(n.token == token);
    Node child = getNode(token, n.right);
    n.right = child.left;
    child.left = n;
    return child;
  }

  private static Node rightRotate(Object token, Node n) {
    if (Debug) expect(n.token == token);
    Node child = getNode(token, n.left);
    n.left = child.right;
    child.right = n;
    return child;
  }

  public static class BlazeResult {
    public Node node;
  }

  public static Node blaze(BlazeResult result,
                           Object token,
                           NodeStack stack,
                           Node root,
                           Object key,
                           Comparator comparator)
  {
    if (key == null) throw new NullPointerException();

    if (root == null) {
      root = Null;
    }

    if (Debug) {
      token = new Object();
      validate(null, root);
    }

    stack = new NodeStack(stack);
    Node newRoot = getNode(token, root);

    Node old = root;
    Node new_ = newRoot;
    while (old != Null) {
      int difference = Compare.compare(key, old.key, comparator);
      if (difference < 0) {
        stack.push(new_);
        old = old.left;
        new_ = new_.left = getNode(token, old);
      } else if (difference > 0) {
        stack.push(new_);
        old = old.right;
        new_ = new_.right = getNode(token, old);
      } else {
        result.node = new_;
        stack.popStack();
        if (Debug) {
          validate(root, newRoot);
        }
        return newRoot;
      }
    }

    new_.key = key;
    result.node = new_;

    // rebalance
    new_.red = true;

    while (stack.top != null && stack.top.red) {
      if (stack.top == stack.peek().left) {
        if (stack.peek().right.red) {
          stack.top.red = false;
          stack.peek().right = getNode(token, stack.peek().right);
          stack.peek().right.red = false;
          stack.peek().red = true;
          new_ = stack.peek();
          stack.pop(2);
        } else {
          if (new_ == stack.top.right) {
            new_ = stack.top;
            stack.pop();

            Node n = leftRotate(token, new_);
            if (stack.top.right == new_) {
              stack.top.right = n;
            } else {
              stack.top.left = n;
            }
            stack.push(n);
          }
          stack.top.red = false;
          stack.peek().red = true;

          Node n = rightRotate(token, stack.peek());
          if (stack.index <= stack.base + 1) {
            newRoot = n;
          } else if (stack.peek(1).right == stack.peek()) {
            stack.peek(1).right = n;
          } else {
            stack.peek(1).left = n;
          }
          // done
        }
      } else {
        // this is just the above code with left and right swapped:
        if (stack.peek().left.red) {
          stack.top.red = false;
          stack.peek().left = getNode(token, stack.peek().left);
          stack.peek().left.red = false;
          stack.peek().red = true;
          new_ = stack.peek();
          stack.pop(2);
        } else {
          if (new_ == stack.top.left) {
            new_ = stack.top;
            stack.pop();

            Node n = rightRotate(token, new_);
            if (stack.top.right == new_) {
              stack.top.right = n;
            } else {
              stack.top.left = n;
            }
            stack.push(n);
          }
          stack.top.red = false;
          stack.peek().red = true;

          Node n = leftRotate(token, stack.peek());
          if (stack.index <= stack.base + 1) {
            newRoot = n;
          } else if (stack.peek(1).right == stack.peek()) {
            stack.peek(1).right = n;
          } else {
            stack.peek(1).left = n;
          }
          // done
        }
      }
    }

    newRoot.red = false;

    stack.popStack();
    if (Debug) {
      validate(root, newRoot);
    }
    return newRoot;
  }

  private static void minimum(Object token,
                              Node n,
                              NodeStack stack)
  {
    while (n.left != Null) {
      n.left = getNode(token, n.left);
      stack.push(n);
      n = n.left;
    }

    stack.push(n);
  }

  private static void successor(Object token,
                                Node n,
                                NodeStack stack)
  {
    if (n.right != Null) {
      n.right = getNode(token, n.right);
      stack.push(n);
      minimum(token, n.right, stack);
    } else {
      while (stack.top != null && n == stack.top.right) {
        n = stack.top;
        stack.pop();
      }
    }
  }

  public static Node delete(Object token,
                            NodeStack stack,
                            Node root,
                            Object key,
                            Comparator comparator)
  {
    if (Debug) {
      token = new Object();
      validate(null, root);
    }

    if (root == Null) {
      return root;
    } else if (root.left == Null && root.right == Null) {
      if (Compare.equal(key, root.key, comparator)) {
        return Null;
      } else {
        return root;
      }
    }

    stack = new NodeStack(stack);
    Node newRoot = getNode(token, root);

    Node old = root;
    Node new_ = newRoot;
    while (old != Null) {
      if (key == null) throw new NullPointerException();
      if (old.key == null) {
        throw new NullPointerException();
      }
      int difference = Compare.compare(key, old.key, comparator);
      if (difference < 0) {
        stack.push(new_);
        old = old.left;
        new_ = new_.left = getNode(token, old);
      } else if (difference > 0) {
        stack.push(new_);
        old = old.right;
        new_ = new_.right = getNode(token, old);
      } else {
        break;
      }
    }

    if (old == Null) {
      if (stack.top.left == new_) {
        stack.top.left = Null;
      } else {
        stack.top.right = Null;
      }
      stack.popStack();
      return root;
    }

    Node dead;
    if (new_.left == Null || new_.right == Null) {
      dead = new_;
    } else {
      successor(token, new_, stack);
      dead = stack.top;
      stack.pop();
    }
    
    Node child;
    if (dead.left != Null) {
      child = getNode(token, dead.left);
    } else if (dead.right != Null) {
      child = getNode(token, dead.right);
    } else {
      child = Null;
    }

    if (stack.top == null) {
      child.red = false;
      stack.popStack();
      if (Debug) {
        validate(root, child);
      }
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
        if (Debug) expect(stack.top.token == token);
        if (child == stack.top.left) {
          Node sibling = stack.top.right = getNode(token, stack.top.right);
          if (sibling.red) {
            if (Debug) expect(sibling.token == token);
            sibling.red = false;
            stack.top.red = true;
            
            Node n = leftRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (stack.peek().right == stack.top) {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().right = n;
            } else {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().left = n;
            }
            Node parent = stack.top;
            stack.top = n;
            stack.push(parent);

            sibling = stack.top.right = getNode(token, stack.top.right);
          }

          if (! (sibling.left.red || sibling.right.red)) {
            if (Debug) expect(sibling.token == token);
            sibling.red = true;
            child = stack.top;
            stack.pop();
          } else {
            if (! sibling.right.red) {
              sibling.left = getNode(token, sibling.left);
              sibling.left.red = false;

              if (Debug) expect(sibling.token == token);
              sibling.red = true;
              sibling = stack.top.right = rightRotate(token, sibling);
            }

            if (Debug) expect(sibling.token == token);
            sibling.red = stack.top.red;
            stack.top.red = false;

            sibling.right = getNode(token, sibling.right);
            sibling.right.red = false;
            
            Node n = leftRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (stack.peek().right == stack.top) {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().right = n;
            } else {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().left = n;
            }

            child = newRoot;
            stack.clear();
          }
        } else {
          // this is just the above code with left and right swapped:
          Node sibling = stack.top.left = getNode(token, stack.top.left);
          if (sibling.red) {
            if (Debug) expect(sibling.token == token);
            sibling.red = false;
            stack.top.red = true;
            
            Node n = rightRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (stack.peek().left == stack.top) {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().left = n;
            } else {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().right = n;
            }
            Node parent = stack.top;
            stack.top = n;
            stack.push(parent);

            sibling = stack.top.left = getNode(token, stack.top.left);
          }

          if (! (sibling.right.red || sibling.left.red)) {
            if (Debug) expect(sibling.token == token);
            sibling.red = true;
            child = stack.top;
            stack.pop();
          } else {
            if (! sibling.left.red) {
              sibling.right = getNode(token, sibling.right);
              sibling.right.red = false;

              if (Debug) expect(sibling.token == token);
              sibling.red = true;
              sibling = stack.top.left = leftRotate(token, sibling);
            }

            if (Debug) expect(sibling.token == token);
            sibling.red = stack.top.red;
            stack.top.red = false;

            sibling.left = getNode(token, sibling.left);
            sibling.left.red = false;
            
            Node n = rightRotate(token, stack.top);
            if (stack.index == stack.base) {
              newRoot = n;
            } else if (stack.peek().left == stack.top) {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().left = n;
            } else {
              if (Debug) expect(stack.peek().token == token);
              stack.peek().right = n;
            }

            child = newRoot;
            stack.clear();
          }
        }
      }

      if (Debug) expect(child.token == token);
      child.red = false;
    }

    stack.popStack();
    if (Debug) {
      validate(root, newRoot);
    }
    return newRoot;
  }

  private static void trouble(Node original, Node n, RuntimeException e) {
    System.err.println("before:");
    if (original != null) dump(original, System.err, 0);
    System.err.println("after:");
    dump(n, System.err, 0);
    throw e;
  }

  public static void validate(@Nullable Node original, Node n) {
    if (original != null) {
      validate(null, original);
    }

    if (Null.red) {
      trouble(original, n, new RuntimeException("red null!"));
    }

    if (Null.left != Null
        || Null.right != Null
        || Null.value != Null
        || Null.key != null)
    {
      trouble(original, n, new RuntimeException("corrupted null!"));
    }

    if (n.red) {
      trouble(original, n, new RuntimeException("red root!"));
    }

    NodeStack s = new NodeStack();
    int blackCount = -1;
    for (NodeIterator it = new NodeIterator(s, n); it.hasNext();) {
      int index = it.stack.index;
      Node x = it.next();

      if (x.key instanceof Node) {
        trouble(original, n, new RuntimeException("node key!"));
      }

      if (x.key == null) {
        trouble(original, n, new RuntimeException("null key!"));
      }

      if (x.value instanceof Node) {
        validate(original, (Node) x.value);
      }

      if (x.red && (x.left.red || x.right.red)) {
        trouble(original, n, new RuntimeException
                ("red node has red child(ren)!"));
      }

      if (x.left == Node.Null && x.right == Node.Null) {
        int count = 0;
        for (int i = 0; i < index; ++i) {
          if (! s.array[i].red) {
            ++ count;
          }
        }
        if (! x.red) {
          ++ count;
        }

        if (blackCount == -1) {
          blackCount = count;
        } else if (count != blackCount) {
          trouble(original, n, new RuntimeException
                  ("inconsistent number of black nodes per paths to leaves!"));
        }
      }
    }
  }

  private static boolean valuesEqual(Node a, Node b) {
    return a != null && b != null && Compare.equal(a.value, b.value);
  }

  public Object value() {
    return (this == Null ? null : value);
  }

  public static boolean treeEqual(NodeStack baseStack,
                                  Node base,
                                  NodeStack forkStack,
                                  Node fork,
                                  Comparator comparator)
  {
    if (base == Node.Null) {
      return fork == Node.Null;
    } else if (fork == Node.Null) {
      return base == Node.Null;
    } else {
      DiffIterator iterator = new DiffIterator
        (base, baseStack = new NodeStack(baseStack),
         fork, forkStack = new NodeStack(forkStack),
         Lists.newArrayList(Interval.Unbounded).iterator(),
         true, comparator);

      DiffIterator.DiffPair pair = new DiffIterator.DiffPair();
      boolean result = true;
      while (iterator.next(pair)) {
        if (! Node.valuesEqual(pair.base, pair.fork)) {
          result = false;
          break;
        }
      }

      baseStack.popStack();
      forkStack.popStack();

      return result;
    }
  }

  public static void dump(Node node, java.io.PrintStream out, int depth) {
    java.io.PrintWriter pw = new java.io.PrintWriter
      (new java.io.OutputStreamWriter(out));

    dump(node, pw, depth);

    pw.flush();
  }

  public static void dump(Node node, java.io.PrintWriter out, int depth) {
    dump(node, out, depth, 0);
  }

  public static void dump(Node node, java.io.PrintWriter out, int depth,
                          int subtreeDepth)
  {
    if (node == Null) {
      return;
    } else {
      dump(node.left, out, depth + 1, subtreeDepth);

      for (int i = 0; i < depth; ++i) {
        out.print("  ");
      }
      out.print(subtreeDepth);
      out.print(" ");
      out.print(node.red ? "(r) " : "(b) ");
      // out.print(System.identityHashCode(node.token));
      // out.print(" ");
      if (node.value instanceof Node) {
        out.println(node.key + ": subtree");
        dump((Node) node.value, out, depth + 2, subtreeDepth + 1);
      } else {
        out.println(node.key + ": " + node.value);
      }

      dump(node.right, out, depth + 1, subtreeDepth);
    }
  }
}
