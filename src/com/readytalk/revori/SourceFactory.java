package com.readytalk.revori;

public class SourceFactory {
  public static Join leftJoin(Source left, Source right) {
    return new Join(Join.Type.LeftOuter, left, right);
  }

  public static Join innerJoin(Source left, Source right) {
    return new Join(Join.Type.Inner, left, right);
  }

  public static TableReference reference(Table table) {
    return new TableReference(table);
  }
}
