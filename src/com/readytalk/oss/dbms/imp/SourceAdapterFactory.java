package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Join;
import com.readytalk.oss.dbms.Source;

import java.util.Map;
import java.util.HashMap;

class SourceAdapterFactory {
  public static final Map<Class<?>, Factory> factories = new HashMap<Class<?>, Factory>();

  static {
    factories.put(TableReference.class, new Factory() {
      public SourceAdapter make(Source source) {
        return new TableAdapter((TableReference) source);
      }
    });

    factories.put(Join.class, new Factory() {
      public SourceAdapter make(Source source) {
        Join join = (Join) source;
        return new JoinAdapter
          (join.type, makeAdapter(join.left), makeAdapter(join.right));
      }
    });
  }

  public static SourceAdapter makeAdapter(Source source) {
    return factories.get(source.getClass()).make(source);
  }

  private interface Factory {
    public SourceAdapter make(Source source);
  }
}
