package com.readytalk.oss.dbms.server;

import com.readytalk.oss.dbms.Revision;

public interface RevisionServer {
  public Revision head();

  public void merge(Revision base, Revision fork);

  public void registerListener(Runnable listener);

  public void unregisterListener(Runnable listener);
}
