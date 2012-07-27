package com.readytalk.revori;

public class Util {
  
  public static void massInsert(RevisionBuilder builder, InsertTemplate insert, QueryResult result) {
    int count = insert.parameterCount();
    Object[] params = new Object[count];
    QueryResult.Type t;
    while((t = result.nextRow()) != QueryResult.Type.End) {
      if(t != QueryResult.Type.Deleted) {
        for(int i = 0; i < count; i++) {
          params[i] = result.nextItem();  
        }
        builder.apply(insert, params);
      }
    }
  }

}
