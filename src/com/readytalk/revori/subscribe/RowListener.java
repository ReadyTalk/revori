package com.readytalk.revori.subscribe;

public interface RowListener {
  public void handleUpdate(Object[] row);
  public void handleDelete(Object[] row);
}
