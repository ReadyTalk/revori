package com.readytalk.revori;

public final class Singleton implements Comparable<Singleton> {
  public static Singleton Instance = new Singleton();

  private Singleton() { }

  public int compareTo(Singleton o) {
    return 0;
  }
}
