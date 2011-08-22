package com.readytalk.oss.dbms;

/**
 * Type representing a join which matches each row in the left source
 * to a row in the right source according to the specified join type.
 */
public class Join implements Source {
  /**
   * These are the possible types which may be specified when defining
   * a join.
   */
  public enum Type {
    /**
     * Indicates a left outer join which matches each row in the left
     * table to a row in the right table (or null if there is no
     * corresponding row).
     */
    LeftOuter,

      /**
       * Indicates an inner join which matches each row in the first
       * table to a row in the second (excluding rows which have no
       * match) according to the specified test.
       */
      Inner;
  }

  /**
   * The type of join to be applied to the operands.
   */
  public final Type type;

  /**
   * The left input operand.
   */
  public final Source left;

  /**
   * The right input operand.
   */
  public final Source right;

  /**
   * Defines a join with the specified type and operands.
   */
  public Join(Type type,
              Source left,
              Source right)
  {
    this.type = type;
    this.left = left;
    this.right = right;
  }

  /**
   * {@inheritDoc}
   */
  public void visit(SourceVisitor visitor) {
    visitor.visit(this);
    visitor.visit(left);
    visitor.visit(right);
  }

  public int compareTo(Source s) {
    if (this == s) return 0;

    if (s instanceof Join) {
      Join o = (Join) s;

      int d = type.compareTo(o.type);
      if (d != 0) {
        return d;
      }
      
      d = left.compareTo(o.left);
      if (d != 0) {
        return d;
      }

      return right.compareTo(o.right);
    } else {
      return getClass().getName().compareTo(s.getClass().getName());
    }
  }

  public boolean equals(Object o) {
    return o instanceof Join && compareTo((Join) o) == 0;
  }
}
