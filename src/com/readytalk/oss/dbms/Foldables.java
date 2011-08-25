package com.readytalk.oss.dbms;

public class Foldables {
  public static final Foldable<Integer> Count = new Foldable<Integer>() {
    public Integer add(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation + 1;
    }

    public Integer subtract(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation - 1;
    }
  };

  public static final Foldable<Integer> Sum = new Foldable<Integer>() {
    public Integer add(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation + (Integer) values[0];
    }

    public Integer subtract(Integer accumulation, Object ... values) {
      if (accumulation == null) accumulation = 0;
      return accumulation - (Integer) values[0];
    }
  };
}
