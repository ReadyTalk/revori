package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.Source;

interface SourceAdapterVisitor {
  public void visit(SourceAdapter source);
}
