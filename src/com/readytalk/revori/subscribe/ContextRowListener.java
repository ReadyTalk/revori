package com.readytalk.revori.subscribe;

public interface ContextRowListener<Context> {
  public void handleUpdate(Context context, Object[] row);
  public void handleDelete(Context context, Object[] row);
}
