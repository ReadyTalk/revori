package com.readytalk.revori.subscribe;

import com.readytalk.revori.QueryTemplate;

class Matcher {
  final RowListener listener;
  final QueryTemplate query;
  final Object[] params;

  public Matcher(RowListener listener, QueryTemplate query, Object[] params) {
    this.listener = listener;
    this.query = query;
    this.params = params;
  }
}