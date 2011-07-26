package com.readytalk.oss.dbms;

/**
 * Interface useful for representing types whose values may be
 * implicitly coerced to other types.<p>
 *
 * Ordinarily, the MyDBMS will only allow, inserts, updates, and
 * comparisons involving types which match the declared column type.
 * However, it is sometimes desirable to compare e.g. 32-bit integers
 * with 64-bit integers, in which case column types extending Coerceable
 * may be used.  As long as an instance of this interface can be
 * coerced to the type needed (e.g. the type of the column into which
 * the value is to be inserted), the operation will succeed.<p>
 *
 * Here's an example of how this interface might be used:
 * <pre>
 * public class Int32 implements Coerceable {
 *   private final int value;

 *   public Int32(int value) {
 *     this.value = value;
 *   }

 *   public Object asType(Class type) {
 *     if (type == Int32.class) {
 *       return this;
 *     } else if (type == Int64.class) {
 *       return new Int64(value);
 *     } else {
 *       throw new ClassCastException
 *         (getClass().getName() + " cannot be coerced to " + type);
 *     }
 *   }

 *   public int compareTo(Coerceable o) {
 *     return value - ((Int32) o.asType(Int32.class)).value;
 *   }

 *   public boolean equals(Object o) {
 *     return o instanceof Coerceable && compareTo((Coerceable) o) == 0;
 *   }

 *   public String toString() {
 *     return String.valueOf(value);
 *   }
 * }

 * public class Int64 implements Coerceable {
 *   private final long value;

 *   public Int64(long value) {
 *     this.value = value;
 *   }

 *   public Object asType(Class type) {
 *     if (type == Int32.class) {
 *       int v = (int) value;
 *       if (v != value) {
 *         throw new RuntimeException
 *           (value + " cannot be represented as a 32-bit value");
 *       }
 *       return new Int32(v);
 *     } else if (type == Int64.class) {
 *       return this;
 *     } else {
 *       throw new ClassCastException
 *         (getClass().getName() + " cannot be coerced to " + type);
 *     }
 *   }

 *   public int compareTo(Coerceable o) {
 *     Int64 ov = (Int64) o.asType(Int64.class);

 *     return value > ov.value ? 1 : (value < ov.value ? -1 : 0);
 *   }

 *   public boolean equals(Object o) {
 *     return o instanceof Coerceable && compareTo((Coerceable) o) == 0;
 *   }

 *   public String toString() {
 *     return String.valueOf(value);
 *   }
 * }</pre>
 */
public interface Coerceable extends Comparable<Coerceable> {
  /**
   * Returns a copy of this value as an instance of the specified
   * type, if possible.
   *
   * @throws ClassCastException if this value cannot be represented as
   * an instance of the specified type.
   */
  public Object asType(Class type);
}
