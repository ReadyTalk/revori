package com.readytalk.oss.dbms;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public final class ForeignKey implements Comparable<ForeignKey> {
  public final Table refererTable;

  public final List<Column<?>> refererColumns;

  public final Table referentTable;

  public final List<Column<?>> referentColumns;

  public ForeignKey(Table refererTable,
                    List<Column<?>> refererColumns,
                    Table referentTable,
                    List<Column<?>> referentColumns)
  {
    this.refererTable = refererTable;
    this.refererColumns = Collections.unmodifiableList
      (new ArrayList<Column<?>>(refererColumns));
    this.referentTable = referentTable;
    this.referentColumns = Collections.unmodifiableList
      (new ArrayList<Column<?>>(referentColumns));

    if (this.refererColumns.size() !=this. referentColumns.size()) {
      throw new IllegalArgumentException
        ("referer column list must be of same length as referent column list");
    }

    if (this.refererColumns.isEmpty()) {
      throw new IllegalArgumentException("column lists must be non-empty");
    }

    Iterator<Column<?>> refererIterator = this.refererColumns.iterator();
    Iterator<Column<?>> referentIterator = this.referentColumns.iterator();
    while (refererIterator.hasNext()) {
      if (refererIterator.next().type != referentIterator.next().type) {
        throw new IllegalArgumentException
          ("type mismatch in referer and referent column lists");
      }
    }
  }

  private static int compare(List<Column<?>> a, List<Column<?>> b) {
    int d = a.size() - b.size();
    if (d != 0) {
      return d;
    }

    Iterator<Column<?>> ai = a.iterator();
    Iterator<Column<?>> bi = b.iterator();
    while (ai.hasNext()) {
      d = ai.next().compareTo(bi.next());
      if (d != 0) {
        return d;
      }
    }

    return 0;
  }

  public int compareTo(ForeignKey o) {
    int d = refererTable.compareTo(o.refererTable);
    if (d != 0) {
      return d;
    }

    d = compare(refererColumns, o.refererColumns);
    if (d != 0) {
      return d;
    }

    d = referentTable.compareTo(o.referentTable);
    if (d != 0) {
      return d;
    }

    return compare(referentColumns, o.referentColumns);
  }

  public int hashCode() {
    int h = refererTable.hashCode() ^ referentTable.hashCode();
    for (Column<?> c: refererColumns) {
      h ^= c.hashCode();
    }
    for (Column<?> c: referentColumns) {
      h ^= c.hashCode();
    }
    return h;
  }

  /**
   * Returns true if and only if the specified object is a ForeignKey
   * and the corresponding tables and columns are equal.
   */
  public boolean equals(Object o) {
    return o instanceof ForeignKey && compareTo((ForeignKey) o) == 0;
  }
      
  public String toString() {
    return "foreignKey[" + refererTable + " " + refererColumns + " "
      + referentTable + " " + referentColumns + "]";
  }
}
