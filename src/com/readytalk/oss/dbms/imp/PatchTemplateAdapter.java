package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.PatchTemplate;

interface PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters);
}
