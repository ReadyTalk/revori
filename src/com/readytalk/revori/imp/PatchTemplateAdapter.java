package com.readytalk.revori.imp;

import com.readytalk.revori.PatchTemplate;

interface PatchTemplateAdapter {
  public int apply(MyRevisionBuilder builder,
                   PatchTemplate template,
                   Object[] parameters);
}
