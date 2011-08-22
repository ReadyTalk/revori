package com.readytalk.oss.dbms;

public class Foldables {
  public static final Foldable<Integer> Count = new Foldable<Integer>() {
    public Integer add(Integer accumulation, Object ... values) {
      return accumulation + 1;
    }

    public Integer subtract(Integer accumulation, Object ... values) {
      return accumulation - 1;
    }
  };
}
