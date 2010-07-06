package com.readytalk.oss.dbms;

import java.util.Collection;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;

public interface DBMS {
  public enum ColumnType {
    Integer32, Integer64, String, ByteArray, Object;
  }

  public interface ByteArray {
    public byte[] array();
    public int offset();
    public int length();
  }

  public interface Column { }

  public interface Table { }

  public interface Database { }

  public interface Expression { }

  public interface Source { }

  public interface BooleanExpression extends Expression { }

  public interface ColumnReference extends Expression { }

  public interface TableReference extends Source { }

  public interface QueryTemplate { }

  public interface Query { }

  public interface PatchTemplate { }

  public interface Patch { }

  public interface QueryResult {
    public boolean nextRow();
    public Object nextItem();
  }

  public interface QueryDiffResult {
    public QueryResult added();
    public QueryResult removed();
  }

  public Column column(ColumnType type);

  public Table table(Collection<Column> columns, List<Column> primaryKey);

  public Database database(Collection<Table> tables);

  public TableReference tableReference(Table table);

  public ColumnReference columnReference(TableReference tableReference,
                                         Column column);

  public BooleanExpression equal(Expression a,
                                 Expression b);

  public Source leftJoin(TableReference left,
                         TableReference right,
                         BooleanExpression constraint);

  public Source innerJoin(TableReference left,
                          TableReference right,
                          BooleanExpression constraint);

  public QueryTemplate queryTemplate(List<Expression> expressions,
                                     Source source,
                                     BooleanExpression constraint);

  public Query query(QueryTemplate template,
                     Object ... parameters);

  public QueryResult execute(Database database,
                             Query query);

  public QueryDiffResult diff(Database base,
                              Database fork,
                              Query query);

  public PatchTemplate insertTemplate(Table table,
                                      List<Column> columns,
                                      List<Expression> values);

  public PatchTemplate updateTemplate(Table table,
                                      BooleanExpression constraint,
                                      List<Column> columns,
                                      List<Expression> values);

  public PatchTemplate deleteTemplate(Table table,
                                      BooleanExpression constraint);

  public Patch patch(PatchTemplate template,
                     Object ... parameters);

  public Database apply(Database database,
                        Patch patch);

  public Database merge(Database base,
                        Database forkA,
                        Database forkB);

  public Patch diff(Database base,
                    Database fork);

  public void write(Patch patch,
                    OutputStream out);

  public Patch readPatch(InputStream in);

  public void write(Database database,
                    OutputStream out);

  public Database readDatabase(InputStream in);
}
