/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori;

import static com.readytalk.revori.util.Util.list;

/**
 * Expression which, when evaluated, applies the specified operation
 * to its operand.
 */
public class UnaryOperation implements Expression {
  /**
   * These are the possible types which may be specified when defining
   * an operation with two operands.
   */
  public enum Type {
    /**
     * Indicates a boolean "not" operation.
     */
    Not(OperationClass.Boolean),

      IsNull(OperationClass.Boolean);

    private final OperationClass operationClass;

    private Type(OperationClass operationClass) {
      this.operationClass = operationClass;
    }

    /**
     * Returns the operation class of this operation.
     */
    public OperationClass operationClass() {
      return operationClass;
    }
  }

  /**
   * The type of operation to be applied to the operands.
   */
  public final Type type;

  /**
   * The input operand.
   */
  public final Expression operand;

  /**
   * Defines an operation with the specified type and operand.
   */
  public UnaryOperation(Type type,
                        Expression operand)
  {
    this.type = type;
    this.operand = operand;
  }

  /**
   * {@inheritDoc}
   */
  public void visit(ExpressionVisitor visitor) {
    visitor.visit(this);
    operand.visit(visitor);
  }

  /**
   * {@inheritDoc}
   */
  public Class typeConstraint() {
    switch (type.operationClass()) {
    case Boolean:
      return Boolean.class;
          
    default: throw new RuntimeException
        ("unexpected operation class: " + type.operationClass());
    }
  }

  /**
   * {@inheritDoc}
   */
  public Iterable<Expression> children() {
    return list(operand);
  }

  public int compareTo(Expression e) {
    if (this == e) return 0;

    if (e instanceof UnaryOperation) {
      UnaryOperation o = (UnaryOperation) e;

      int d = type.compareTo(o.type);
      if (d != 0) {
        return d;
      }

      return operand.compareTo(o.operand);
    } else {
      return getClass().getName().compareTo(e.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof UnaryOperation && compareTo((UnaryOperation) o) == 0;
  }
}
