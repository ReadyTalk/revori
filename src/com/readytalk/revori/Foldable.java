package com.readytalk.revori;

public interface Foldable<T> {
  public T add(T accumulation, Object ... values);
  
  public T subtract(T accumulation, Object ... values);
}
