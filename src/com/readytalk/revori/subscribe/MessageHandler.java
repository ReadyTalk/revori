package com.readytalk.revori.subscribe;

public interface MessageHandler<T> {
  public void handleMessage(T message);
}
