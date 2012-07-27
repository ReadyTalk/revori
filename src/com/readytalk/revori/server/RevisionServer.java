package com.readytalk.revori.server;

import com.readytalk.revori.Revision;

public interface RevisionServer {
  public Revision head();

  public void merge(Revision base, Revision fork);

  public void registerListener(Runnable listener);

  public void unregisterListener(Runnable listener);
}
