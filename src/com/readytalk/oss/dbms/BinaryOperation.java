package com.readytalk.oss.dbms;

/**
 * Expression which, when evaluated, applies the specified operation
 * to its operands.
 */
public class BinaryOperation implements Expression {
  /**
   * These are the possible types which may be specified when defining
   * an operation with two operands.
   */
  public enum Type {
    /**
     * Indicates a boolean "and" operation.
     */
    And(OperationClass.Boolean),

      /**
       * Indicates a boolean "or" operation.
       */
      Or(OperationClass.Boolean),

      /**
       * Indicates a comparison which evaluates to true if the
       * operands are equal.
       */
      Equal(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the
       * operands are not equal.
       */
      NotEqual(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares less than the right.
       */
      LessThan(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares less than or equal to the right.
       */
      LessThanOrEqual(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares greater than the right.
       */
      GreaterThan(OperationClass.Comparison),

      /**
       * Indicates a comparison which evaluates to true if the left
       * operand compares greater than or equal to the right.
       */
      GreaterThanOrEqual(OperationClass.Comparison);

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
   * The left input operand.
   */
  public final Expression leftOperand;

  /**
   * The right input operand.
   */
  public final Expression rightOperand;

  /**
   * Defines an operation with the specified type and operands.
   */
  public BinaryOperation(Type type,
                         Expression leftOperand,
                         Expression rightOperand)
  {
    this.type = type;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  /**
   * {@inheritDoc}
   */
  public void visit(ExpressionVisitor visitor) {
    visitor.visit(this);
    leftOperand.visit(visitor);
    rightOperand.visit(visitor);
  }

  /**
   * {@inheritDoc}
   */
  public Class typeConstraint() {
    switch (type.operationClass()) {
    case Boolean:
    case Comparison:
      return Boolean.class;
          
    default: throw new RuntimeException
        ("unexpected operation class: " + type.operationClass());
    }
  }
}
