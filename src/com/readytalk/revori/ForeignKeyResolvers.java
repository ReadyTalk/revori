package com.readytalk.revori;

public class ForeignKeyResolvers {
  public static final ForeignKeyResolver Restrict = new ForeignKeyResolver() {
      public ForeignKeyResolver.Action handleBrokenReference
        (ForeignKey constraint,
         Object[] refererRowPrimaryKeyValues)
      {
        return ForeignKeyResolver.Action.Restrict;
      }
    };

  public static final ForeignKeyResolver Delete = new ForeignKeyResolver() {
      public ForeignKeyResolver.Action handleBrokenReference
        (ForeignKey constraint,
         Object[] refererRowPrimaryKeyValues)
      {
        return ForeignKeyResolver.Action.Delete;
      }
    };
}
