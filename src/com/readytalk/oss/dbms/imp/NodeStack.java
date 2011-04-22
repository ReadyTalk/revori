package com.readytalk.oss.dbms.imp;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.expect;

class NodeStack {
  public static final NodeStack Null = new NodeStack((Node[]) null);

  private static final int Size = 64;

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
    this(new Node[Size]);
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

  public NodeStack popStack() {
    expect(previous == null);
    expect(array == null || next.previous == this);

    NodeStack s = next;
    s.previous = null;

    return s;
  }

  public void push(Node n) {
    if (top != null) {
      array[index++] = top;
    }
    top = n;
  }

  public Node peek(int depth) {
    expect(index - depth > base);

    return array[index - depth - 1];
  }

  public Node peek() {
    return peek(0);
  }

  public void pop(int count) {
    expect(count > 0);
    expect(top != null);

    if (index - count < base) {
      expect(index - count == base - 1);

      index -= count - 1;
      top = null;
    } else {
      expect(index - count >= base);

      index -= count;
      top = array[index];
    }
  }

  public void pop() {
    pop(1);
  }

  public void clear() {
    top = null;
    index = base;
  }

  public void descend(int oppositeDirection) {
    if (oppositeDirection > 0) {
      push(top.left);
    } else {
      push(top.right);
    }
  }

  public void descendToLeftmost() {
    while (top.left != Node.Null) {
      push(top.left);
    }
  }

  public void next() {
    if (top != null) {
      if (top.right != Node.Null) {
        push(top.right);

        while (top.left != Node.Null) {
          push(top.left);
        }
      } else {
        ascendNext();
      }
    }
  }

  public void ascendNext() {
    while (index != base && peek().right == top) {
      pop();
    }

    if (index == base) {
      clear();
    } else {
      pop();
    }
  }

  public void descendLeft() {
    if (top != null && top.left != Node.Null) {
      push(top.left);
    }
  }
}
