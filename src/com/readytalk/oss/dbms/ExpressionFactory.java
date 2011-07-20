package com.readytalk.oss.dbms;

/**
 * This class contains static convenience methods for concisely
 * defining expression trees.
 */
public class ExpressionFactory {
  /**
   * Alias for <code>new Parameter()</code>
   */
  public static Expression parameter() {
    return new Parameter();
  }

  /**
   * Alias for <code>new Constant(value)</code>
   */
  public static Expression constant(Object value) {
    return new Constant(value);
  }

  /**
   * Alias for <code>new ColumnReference(tableReference, column)</code>
   */
  public static Expression reference(TableReference tableReference,
                                     Column column)
  {
    return new ColumnReference(tableReference, column);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.And,
   * left, right)</code>
   */
  public static Expression and(Expression left,
                               Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.And, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.Or,
   * left, right)</code>
   */
  public static Expression or(Expression left,
                              Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.Or, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.Equal,
   * left, right)</code>
   */
  public static Expression equal(Expression left,
                                 Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.Equal, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.NotEqual,
   * left, right)</code>
   */
  public static Expression notEqual(Expression left,
                                    Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.NotEqual, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.LessThan,
   * left, right)</code>
   */
  public static Expression lessThan(Expression left,
                                    Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.LessThan, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.LessThanOrEqual,
   * left, right)</code>
   */
  public static Expression lessThanOrEqual(Expression left,
                                           Expression right)
  {
    return new BinaryOperation
      (BinaryOperation.Type.LessThanOrEqual, left, right);
  }

  /**
   * Alias for <code>new BinaryOperation(BinaryOperation.Type.GreaterThan,
   * left, right)</code>
   */
  public static Expression greaterThan(Expression left,
                                       Expression right)
  {
    return new BinaryOperation(BinaryOperation.Type.GreaterThan, left, right);
  }

  /**
   * Alias for <code>new
   * BinaryOperation(BinaryOperation.Type.GreaterThanOrEqual, left,
   * right)</code>
   */
  public static Expression greaterThanOrEqual(Expression left,
                                              Expression right)
  {
    return new BinaryOperation
      (BinaryOperation.Type.GreaterThanOrEqual, left, right);
  }

  /**
   * Alias for <code>new UnaryOperation(UnaryOperation.Type.Not,
   * operand)</code>
   */
  public static Expression not(Expression operand) {
    return new UnaryOperation(UnaryOperation.Type.Not, operand);
  }

  public static Expression isNull(Expression operand) {
    return new UnaryOperation(UnaryOperation.Type.IsNull, operand);
  }
}
