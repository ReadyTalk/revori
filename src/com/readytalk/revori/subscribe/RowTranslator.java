package com.readytalk.revori.subscribe;

public interface RowTranslator<T> {
  public void handleUpdate(MessageHandler<T> handler, Object[] row);
  public void handleDelete(MessageHandler<T> handler, Object[] row);
}
