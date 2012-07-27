package com.readytalk.revori.imp;

import com.readytalk.revori.QueryResult;

interface SourceIterator {
  public QueryResult.Type nextRow();
  public boolean rowUpdated();
}
