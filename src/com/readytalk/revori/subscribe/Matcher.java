package com.readytalk.revori.subscribe;

import com.readytalk.revori.QueryTemplate;

class Matcher<Context> {
  final ContextRowListener<Context> listener;
  final QueryTemplate query;
  final Object[] params;

  public Matcher(ContextRowListener<Context> listener,
                 QueryTemplate query,
                 Object[] params)
  {
    this.listener = listener;
    this.query = query;
    this.params = params;
  }
}
