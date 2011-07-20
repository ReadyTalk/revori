package com.readytalk.oss.dbms;

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
}
