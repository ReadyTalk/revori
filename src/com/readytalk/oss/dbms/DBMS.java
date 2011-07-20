package com.readytalk.oss.dbms;

/**
 * @deprecated
 */
@Deprecated
public interface DBMS {
  /**
   * @deprecated
   */
  @Deprecated
  public Revision revision();

  /**
   * @deprecated
   */
  @Deprecated
  public QueryResult diff(Revision base,
                          Revision fork,
                          QueryTemplate template,
                          Object ... parameters);

  /**
   * @deprecated
   */
  @Deprecated
  public DiffResult diff(Revision base,
                         Revision fork);

  /**
   * @deprecated
   */
  @Deprecated
  public RevisionBuilder builder(Revision base);

  /**
   * @deprecated
   */
  @Deprecated
  public Revision merge(Revision base,
                        Revision left,
                        Revision right,
                        ConflictResolver conflictResolver,
                        ForeignKeyResolver foreignKeyResolver);
}
