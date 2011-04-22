package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.QueryResult;

interface SourceIterator {
  public QueryResult.Type nextRow();
  public boolean rowUpdated();
}
