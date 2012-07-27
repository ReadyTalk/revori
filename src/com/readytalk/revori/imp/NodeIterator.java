/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import java.util.NoSuchElementException;

class NodeIterator {
  public NodeStack stack; // not final only so we can assign to null (for safety)
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
      stack = null; // for safety
      hasNext = false;
    }

    return n;
  }
}
