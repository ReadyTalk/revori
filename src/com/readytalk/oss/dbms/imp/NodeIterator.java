package com.readytalk.oss.dbms.imp;

import java.util.NoSuchElementException;

class NodeIterator {
  public final NodeStack stack;
  public boolean hasNext;
    
  public NodeIterator(NodeStack stack,
                      Node root)
  {
    if (root != Node.Null) {
      this.stack = new NodeStack(stack);
      this.stack.push(root);
      this.stack.descendToLeftmost();
      hasNext = true;
    } else {
      this.stack = null;
      hasNext = false;
    }
  }

  public boolean hasNext() {
    return hasNext;
  }

  public Node next() {
    if (! hasNext) {
      throw new NoSuchElementException();
    }

    Node n = stack.top;
    stack.next();
      
    if (stack.top == null) {
      stack.popStack();
      hasNext = false;
    }

    return n;
  }
}