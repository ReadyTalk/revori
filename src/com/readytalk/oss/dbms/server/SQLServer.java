package com.readytalk.oss.dbms.server;

import static com.readytalk.oss.dbms.imp.Util.list;
import static com.readytalk.oss.dbms.imp.Util.set;

import com.readytalk.oss.dbms.imp.Util;
import com.readytalk.oss.dbms.imp.MyDBMS;
import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.Index;
import com.readytalk.oss.dbms.DBMS.ColumnReference;
import com.readytalk.oss.dbms.DBMS.Source;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.JoinType;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.UnaryOperationType;
import com.readytalk.oss.dbms.DBMS.Row;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;

public class SQLServer {
  private static final Logger log = Logger.getLogger("SQLServer");

  public enum Request {
    Execute, Complete;
  }

  public enum Response {
    RowSet, Success, Error;
  }

  public enum RowSetFlag {
    InsertedRow, DeletedRow, End, Item;
  }

  private static final int ThreadPoolSize = 256;

  private static final Tree Nothing = new Leaf();
  private static final Set<Index> EmptyIndexSet = new HashSet();

  private static class Server {
    private final DBMS dbms;

    private final Parser parser = new ParserFactory().parser();

    private final PatchTemplate insertOrUpdateDatabase;
    private final QueryTemplate listDatabases;
    private final QueryTemplate findDatabase;
    private final PatchTemplate deleteDatabase;

    private final PatchTemplate insertOrUpdateTable;
    private final QueryTemplate listTables;
    private final QueryTemplate findTable;
    private final PatchTemplate deleteDatabaseTables;
    private final PatchTemplate deleteTable;

    private final DBMS.Column tagsDatabase;
    private final DBMS.Column tagsName;
    private final DBMS.Column tagsTag;
    private final DBMS.Table tags;

    private final PatchTemplate insertOrUpdateTag;
    private final QueryTemplate listTags;
    private final QueryTemplate findTag;
    private final PatchTemplate deleteDatabaseTags;
    private final PatchTemplate deleteTag;

    private final AtomicReference<Revision> dbHead;
    private final Map<String, BinaryOperationType> binaryOperationTypes
      = new HashMap();
    private final Map<String, UnaryOperationType> unaryOperationTypes
      = new HashMap();
    private final Map<String, Class> columnTypes = new HashMap();
    private final ConflictResolver rightPreferenceConflictResolver
      = new ConflictResolver() {
          public Row resolveConflict(DBMS.Table table,
                                     Collection<DBMS.Column> columns,
                                     Revision base,
                                     Row baseRow,
                                     Revision left,
                                     Row leftRow,
                                     Revision right,
                                     Row rightRow)
          {
            return leftPreferenceMerge(columns, baseRow, rightRow, leftRow);
          }
        };
    private final ConflictResolver conflictResolver = new ConflictResolver() {
        public Row resolveConflict(DBMS.Table table,
                                   Collection<DBMS.Column> columns,
                                   Revision base,
                                   Row baseRow,
                                   Revision left,
                                   Row leftRow,
                                   Revision right,
                                   Row rightRow)
        {
          if (table == tags) {
            HashRow result = new HashRow(3);
            result.put(tagsDatabase, leftRow.value(tagsDatabase));
            result.put(tagsName, leftRow.value(tagsName));
            result.put(tagsTag, new Tag
                       ((String) leftRow.value(tagsName), dbms.merge
                        (baseRow == null
                         ? dbms.revision()
                         : ((Tag) baseRow.value(tagsTag)).revision,
                         ((Tag) leftRow.value(tagsTag)).revision,
                         ((Tag) rightRow.value(tagsTag)).revision,
                         rightPreferenceConflictResolver)));
            return result;
          } else {
            return leftPreferenceMerge(columns, baseRow, rightRow, leftRow);
          }
        }
      };

    public Server(DBMS dbms) {
      this.dbms = dbms;

      DBMS.Column databasesName = dbms.column(String.class);
      DBMS.Column databasesDatabase = dbms.column(Object.class);
      DBMS.Table databases = dbms.table
        (set(databasesName, databasesDatabase),
         dbms.index(list(databasesName), true),
         EmptyIndexSet);
      DBMS.TableReference databasesReference = dbms.tableReference(databases);

      this.insertOrUpdateDatabase = dbms.insertTemplate
        (databases, list(databasesName, databasesDatabase),
         list(dbms.parameter(), dbms.parameter()), true);

      this.listDatabases = dbms.queryTemplate
        (list((Expression) dbms.columnReference
              (databasesReference, databasesDatabase)),
         databasesReference,
         dbms.constant(true));

      this.findDatabase = dbms.queryTemplate
        (list((Expression) dbms.columnReference
              (databasesReference, databasesDatabase)),
         databasesReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(databasesReference, databasesName),
          dbms.parameter()));

      this.deleteDatabase = dbms.deleteTemplate
        (databasesReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(databasesReference, databasesName),
          dbms.parameter()));

      DBMS.Column tablesDatabase = dbms.column(String.class);
      DBMS.Column tablesName = dbms.column(String.class);
      DBMS.Column tablesTable = dbms.column(Object.class);
      DBMS.Table tables = dbms.table
        (set(tablesDatabase, tablesName, tablesTable),
         dbms.index(list(tablesDatabase, tablesName), true),
         EmptyIndexSet);
      DBMS.TableReference tablesReference = dbms.tableReference(tables);

      this.insertOrUpdateTable = dbms.insertTemplate
        (tables, list(tablesDatabase, tablesName, tablesTable),
         list(dbms.parameter(), dbms.parameter(), dbms.parameter()), true);

      this.listTables = dbms.queryTemplate
        (list((Expression) dbms.columnReference(tablesReference, tablesTable)),
         tablesReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(tablesReference, tablesDatabase),
          dbms.parameter()));

      this.findTable = dbms.queryTemplate
        (list((Expression) dbms.columnReference(tablesReference, tablesTable)),
         tablesReference,
         dbms.operation
         (BinaryOperationType.And,
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tablesReference, tablesDatabase),
           dbms.parameter()),
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tablesReference, tablesName),
           dbms.parameter())));

      this.deleteDatabaseTables = dbms.deleteTemplate
        (tablesReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(tablesReference, tablesDatabase),
          dbms.parameter()));

      this.deleteTable = dbms.deleteTemplate
        (tablesReference,
         dbms.operation
         (BinaryOperationType.And,
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tablesReference, tablesDatabase),
           dbms.parameter()),
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tablesReference, tablesName),
           dbms.parameter())));

      this.tagsDatabase = dbms.column(String.class);
      this.tagsName = dbms.column(String.class);
      this.tagsTag = dbms.column(Object.class);
      this.tags = dbms.table
        (set(tagsDatabase, tagsName, tagsTag),
         dbms.index(list(tagsDatabase, tagsName), true),
         EmptyIndexSet);
      DBMS.TableReference tagsReference = dbms.tableReference(tags);

      this.insertOrUpdateTag = dbms.insertTemplate
        (tags, list(tagsDatabase, tagsName, tagsTag),
         list(dbms.parameter(), dbms.parameter(), dbms.parameter()), true);

      this.listTags = dbms.queryTemplate
        (list((Expression) dbms.columnReference(tagsReference, tagsTag)),
         tagsReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(tagsReference, tagsDatabase),
          dbms.parameter()));

      this.findTag = dbms.queryTemplate
        (list((Expression) dbms.columnReference(tagsReference, tagsTag)),
         tagsReference,
         dbms.operation
         (BinaryOperationType.And,
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tagsReference, tagsDatabase),
           dbms.parameter()),
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tagsReference, tagsName),
           dbms.parameter())));

      this.deleteDatabaseTags = dbms.deleteTemplate
        (tagsReference,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(tagsReference, tagsDatabase),
          dbms.parameter()));

      this.deleteTag = dbms.deleteTemplate
        (tagsReference,
         dbms.operation
         (BinaryOperationType.And,
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tagsReference, tagsDatabase),
           dbms.parameter()),
          dbms.operation
          (BinaryOperationType.Equal,
           dbms.columnReference(tagsReference, tagsName),
           dbms.parameter())));

      this.dbHead = new AtomicReference(dbms.revision());

      binaryOperationTypes.put("and", BinaryOperationType.And);
      binaryOperationTypes.put("or", BinaryOperationType.Or);
      binaryOperationTypes.put("=", BinaryOperationType.Equal);
      binaryOperationTypes.put("<>", BinaryOperationType.NotEqual);
      binaryOperationTypes.put(">", BinaryOperationType.GreaterThan);
      binaryOperationTypes.put(">=", BinaryOperationType.GreaterThanOrEqual);
      binaryOperationTypes.put("<", BinaryOperationType.LessThan);
      binaryOperationTypes.put("<=", BinaryOperationType.LessThanOrEqual);

      unaryOperationTypes.put("not", UnaryOperationType.Not);

      columnTypes.put("int32", Integer.class);
      columnTypes.put("int64", Long.class);
      columnTypes.put("string", String.class);
    }
  }

  private static boolean equal(Object left,
                               Object right)
  {
    return left == right || (left != null && left.equals(right));
  }

  private static Row leftPreferenceMerge(Collection<DBMS.Column> columns,
                                         Row base,
                                         Row left,
                                         Row right)
  {
    if (base == null) {
      return left;
    } else {
      HashRow result = new HashRow(columns.size());
      for (DBMS.Column c: columns) {
        Object baseItem = base.value(c);
        Object leftItem = left.value(c);
        Object rightItem = right.value(c);

        if (equal(baseItem, leftItem)) {
          result.put(c, rightItem);
        } else {
          result.put(c, leftItem);
        }
      }
      return result;
    }
  }

  private static class LeftPreferenceConflictResolver
    implements ConflictResolver
  {
    private int conflictCount;

    public Row resolveConflict(DBMS.Table table,
                               Collection<DBMS.Column> columns,
                               Revision base,
                               Row baseRow,
                               Revision left,
                               Row leftRow,
                               Revision right,
                               Row rightRow)
    {
      ++ conflictCount;
      return leftPreferenceMerge(columns, baseRow, leftRow, rightRow);
    }
  }

  private static class HashRow implements Row {
    private final Map<DBMS.Column, Object> values;

    public HashRow(int size) {
      this.values = new HashMap(size);
    }

    public void put(DBMS.Column column, Object value) {
      values.put(column, value);
    }

    public Object value(DBMS.Column column) {
      return values.get(column);
    }
  }

  private static class Transaction {
    private final Transaction next;
    private final Revision dbTail;
    private Revision dbHead;

    public Transaction(Transaction next,
                       Revision dbTail)
    {
      this.next = next;
      this.dbTail = dbTail;
      this.dbHead = dbTail;
    }
  }

  private static class Client implements Runnable {
    private final Server server;
    private final SocketChannel channel;
    private Transaction transaction;
    private Database database;

    public Client(Server server,
                  SocketChannel channel)
    {
      this.server = server;
      this.channel = channel;
    }

    public void run() {
      try {
        try {
          InputStream in = new BufferedInputStream
            (Channels.newInputStream(channel));

          OutputStream out = new BufferedOutputStream
            (Channels.newOutputStream(channel));

          while (channel.isOpen()) {
            handleRequest(this, in, out);
          }
        } finally {
          channel.close();
        }
      } catch (Exception e) {
        log.log(Level.WARNING, null, e);
      }
    }
  }

  private enum NameType {
    Database, Table, Column, Tag;
  }

  private static class Database {
    private final String name;

    public Database(String name) {
      this.name = name;
    }
  }

  private static class Column {
    private final String name;
    private final DBMS.Column column;
    private final Class type;
    
    public Column(String name,
                  DBMS.Column column,
                  Class type)
    {
      this.name = name;
      this.column = column;
      this.type = type;
    }
  }

  private static class Table {
    private final String name;
    private final Map<String, Column> columns;
    private final List<Column> primaryKeyColumns;
    private final DBMS.Table table;

    public Table(String name,
                 Map<String, Column> columns,
                 List<Column> primaryKeyColumns,
                 DBMS.Table table)
    {
      this.name = name;
      this.columns = columns;
      this.primaryKeyColumns = primaryKeyColumns;
      this.table = table;
    }
  }

  private static class Tag {
    private final String name;
    private final Revision revision;

    public Tag(String name,
               Revision revision)
    {
      this.name = name;
      this.revision = revision;
    }
  }
  
  private static class TableReference {
    private final Table table;
    private final DBMS.TableReference reference;

    public TableReference(Table table,
                          DBMS.TableReference reference)
    {
      this.table = table;
      this.reference = reference;
    }
  }

  private static class ParseResult {
    public final Tree tree;
    public final String next;
    public final List<String> completions;
    public Task task;

    public ParseResult(Tree tree,
                       String next,
                       List<String> completions)
    {
      this.tree = tree;
      this.next = next;
      this.completions = completions;
    }
  }

  private static class ParseContext {
    public final Client client;
    public Map<NameType, List<String>> completions;

    public ParseContext(Client client) {
      this.client = client;
    }
  }

  private interface Parser {
    public ParseResult parse(ParseContext context, String in);
  }

  private static class LazyParser implements Parser {
    public Parser parser;

    public ParseResult parse(ParseContext context, String in) {
      return parser.parse(context, in);
    }
  }

  private interface Tree {
    public Tree get(int index);
    public int length();
  }

  private static class TreeList implements Tree {
    private final List<Tree> list = new ArrayList();
    
    public void add(Tree tree) {
      list.add(tree);
    }

    public Tree get(int index) {
      return list.get(index);
    }

    public int length() {
      return list.size();
    }
  }

  private static class Leaf implements Tree {
    public Tree get(int index) {
      throw new UnsupportedOperationException();
    }

    public int length() {
      throw new UnsupportedOperationException();
    }
  }

  private static class Terminal extends Leaf {
    public final String value;

    public Terminal(String value) {
      this.value = value;
    }
  }

  private static class Name extends Leaf {
    public final String value;

    public Name(String value) {
      this.value = value;
    }
  }

  private static class StringLiteral extends Leaf {
    public final String value;

    public StringLiteral(String value) {
      this.value = value;
    }
  }

  private static class NumberLiteral extends Leaf {
    public final long value;

    public NumberLiteral(String value) {
      this.value = Long.parseLong(value);
    }
  }

  private interface Task {
    public void run(Client client,
                    Tree tree,
                    InputStream in,
                    OutputStream out)
      throws IOException;
  }

  private static Revision dbHead(Client client) {
    if (client.transaction != null) {
      return client.transaction.dbHead;
    } else {
      return client.server.dbHead.get();
    }
  }

  private static Database findDatabase(Client client,
                                       String name)
  {
    Server server = client.server;
    DBMS dbms = server.dbms;
    QueryResult result = dbms.diff
      (dbms.revision(), dbHead(client), server.findDatabase, name);

    if (result.nextRow() == ResultType.Inserted) {
      return (Database) result.nextItem();
    } else {
      throw new RuntimeException("no such database: " + name);
    }
  }

  private static Tag findTag(Client client,
                             String name)
  {
    Server server = client.server;
    DBMS dbms = server.dbms;
    QueryResult result = dbms.diff
      (dbms.revision(), dbHead(client), server.findTag, database(client).name,
       name);

    if (result.nextRow() == ResultType.Inserted) {
      return (Tag) result.nextItem();
    } else {
      throw new RuntimeException("no such tag: " + name);
    }
  }

  private static Revision head(Client client) {
    return findTag(client, "head").revision;
  }

  private static Database database(Client client) {
    if (client.database == null) {
      throw new RuntimeException("no database specified");
    } else {
      return client.database;
    }
  }

  private static Table findTable(Client client,
                                 String name)
  {
    DBMS dbms = client.server.dbms;
    QueryResult result = dbms.diff
      (dbms.revision(), dbHead(client), client.server.findTable,
       database(client).name, name);

    if (result.nextRow() == ResultType.Inserted) {
      return (Table) result.nextItem();
    } else {
      throw new RuntimeException("no such table: " + name);
    }
  }

  private static Source makeSource(Client client,
                                   Tree tree,
                                   List<TableReference> tableReferences,
                                   List<Expression> tests)
  {
    if (tree instanceof Name) {
      Table table = findTable(client, ((Name) tree).value);
      DBMS.TableReference reference = client.server.dbms.tableReference
        (table.table);
      tableReferences.add(new TableReference(table, reference));
      return reference;
    } else if (tree.get(0) instanceof Terminal) {
      return makeSource(client, tree.get(1), tableReferences, tests);
    } else {
      tests.add(makeExpression(client.server, tree.get(4), tableReferences));

      return client.server.dbms.join
        ("left".equals(((Terminal) tree.get(1).get(0)).value)
         ? JoinType.LeftOuter : JoinType.Inner,
         makeSource(client, tree.get(0), tableReferences, tests),
         makeSource(client, tree.get(2), tableReferences, tests));
    }
  }

  private static ColumnReference makeColumnReference
    (DBMS dbms,
     List<TableReference> tableReferences,
     String tableName,
     String columnName)
  {
    DBMS.TableReference reference = null;
    DBMS.Column column = null;
    for (TableReference r: tableReferences) {
      if (tableName == null || tableName.equals(r.table.name)) {
        Column c = r.table.columns.get(columnName);
        if (c != null) {
          if (column != null) {
            throw new RuntimeException("ambiguous column name: " + columnName);
          } else {
            reference = r.reference;
            column = c.column;

            if (tableName != null) {
              break;
            }
          }
        }
      }
    }

    if (column == null) {
      throw new RuntimeException("no such column: " + columnName);
    }

    return dbms.columnReference(reference, column);
  }

  private static UnaryOperationType findUnaryOperationType(Server server,
                                                           String name)
  {
    return server.unaryOperationTypes.get(name);
  }

  private static BinaryOperationType findBinaryOperationType(Server server,
                                                             String name)
  {
    return server.binaryOperationTypes.get(name);
  }

  private static Expression makeExpression
    (Server server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    if (tree instanceof Name) {
      return makeColumnReference
        (server.dbms, tableReferences, null, ((Name) tree).value);
    } else if (tree instanceof StringLiteral) {
      return server.dbms.constant(((StringLiteral) tree).value);
    } else if (tree instanceof NumberLiteral) {
      return server.dbms.constant(((NumberLiteral) tree).value);
    } if (tree.get(0) instanceof Name) {
      return makeColumnReference
        (server.dbms,
         tableReferences,
         ((Name) tree.get(0)).value,
         ((Name) tree.get(2)).value);
    } else if (tree.get(0) instanceof Terminal) {
      String value = ((Terminal) tree.get(0)).value;
      if ("(".equals(value)) {
        return makeExpression(server, tree.get(1), tableReferences);
      } else {
        return server.dbms.operation
          (findUnaryOperationType(server, value), makeExpression
           (server, tree.get(1), tableReferences));
      }
    } else {
      return server.dbms.operation
        (findBinaryOperationType(server, ((Terminal) tree.get(1)).value),
         makeExpression(server, tree.get(0), tableReferences),
         makeExpression(server, tree.get(2), tableReferences));
    }
  }

  private static List<Expression> makeExpressionList
    (Server server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    List<Expression> expressions = new ArrayList();
    if (tree instanceof Terminal) {
      DBMS dbms = server.dbms;
      for (TableReference tableReference: tableReferences) {
        for (Column column: tableReference.table.columns.values()) {
          expressions.add
            (dbms.columnReference(tableReference.reference, column.column));
        }
      }
    } else {
      for (int i = 0; i < tree.length(); ++i) {
        expressions.add
          (makeExpression(server, tree.get(i), tableReferences));
      }
    }
    return expressions;
  }

  private static Expression makeExpressionFromWhere
    (Server server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    if (tree == Nothing) {
      return server.dbms.constant(true);
    } else {
      return makeExpression(server, tree.get(1), tableReferences);
    }
  }

  private static Expression andExpressions(DBMS dbms,
                                           Expression expression,
                                           List<Expression> expressions)
  {
    for (Expression e: expressions) {
      expression = dbms.operation(BinaryOperationType.And, expression, e);
    }
    return expression;
  }

  private static QueryTemplate makeQueryTemplate(Client client,
                                                 Tree tree,
                                                 int[] expressionCount)
  {
    List<TableReference> tableReferences = new ArrayList();
    List<Expression> tests = new ArrayList();
    Source source = makeSource
      (client, tree.get(3), tableReferences, tests);

    List<Expression> expressions = makeExpressionList
      (client.server, tree.get(1), tableReferences);

    expressionCount[0] = expressions.size();

    return client.server.dbms.queryTemplate
      (expressions, source, andExpressions
       (client.server.dbms, makeExpressionFromWhere
        (client.server, tree.get(4), tableReferences), tests));
  }

  private static Column findColumn(Table table,
                                   String name)
  {
    Column c = table.columns.get(name);
    if (c == null) {
      throw new RuntimeException("no such column: " + name);
    } else {
      return c;
    }
  }

  private static List<DBMS.Column> makeColumnList(Table table,
                                                  Tree tree)
  {
    List<DBMS.Column> columns = new ArrayList(tree.length());
    for (int i = 0; i < tree.length(); ++i) {
      columns.add(findColumn(table, ((Name) tree.get(i)).value).column);
    }
    return columns;
  }

  private static List<DBMS.Column> makeOptionalColumnList(Table table,
                                                          Tree tree)
  {
    if (tree == Nothing) {
      List<DBMS.Column> columns = new ArrayList(table.columns.size());
      for (Column c: table.columns.values()) {
        columns.add(c.column);
      }
      return columns;
    } else {
      return makeColumnList(table, tree.get(1));
    }
  }

  private static PatchTemplate makeInsertTemplate(Client client,
                                                  Tree tree)
  {
    Table table = findTable(client, ((Name) tree.get(2)).value);

    return client.server.dbms.insertTemplate
      (table.table, makeOptionalColumnList(table, tree.get(3)),
       makeExpressionList(client.server, tree.get(6), null), false);
  }

  private static PatchTemplate makeUpdateTemplate(Client client,
                                                  Tree tree)
  {
    Table table = findTable(client, ((Name) tree.get(2)).value);
    TableReference tableReference = new TableReference
      (table, client.server.dbms.tableReference(table.table));
    List<TableReference> tableReferences = list(tableReference);

    Tree operations = tree.get(3);
    List<DBMS.Column> columns = new ArrayList();
    List<Expression> values = new ArrayList();
    for (int i = 0; i < tree.length(); ++i) {
      Tree operation = tree.get(i);
      columns.add(findColumn(table, ((Name) operation.get(0)).value).column);
      values.add
        (makeExpression(client.server, operation.get(2), tableReferences));
    }

    return client.server.dbms.updateTemplate
      (tableReference.reference, makeExpressionFromWhere
       (client.server, tree.get(4), tableReferences), columns, values);
  }

  private static PatchTemplate makeDeleteTemplate(Client client,
                                                  Tree tree)
  {
    Table table = findTable(client, ((Name) tree.get(2)).value);
    TableReference tableReference = new TableReference
      (table, client.server.dbms.tableReference(table.table));
    List<TableReference> tableReferences = list(tableReference);

    return client.server.dbms.deleteTemplate
      (tableReference.reference, makeExpressionFromWhere
       (client.server, tree.get(4), tableReferences));
  }

  private static Column findColumn(Table table, DBMS.Column column) {
    for (Column c: table.columns.values()) {
      if (c.column == column) {
        return c;
      }
    }
    throw new RuntimeException();
  }

  private static PatchTemplate makeCopyTemplate(Client client,
                                                Tree tree,
                                                List<Class> columnTypes)
  {
    Table table = findTable(client, ((Name) tree.get(1)).value);

    List<DBMS.Column> columns = makeOptionalColumnList(table, tree.get(2));
    List<Expression> values = new ArrayList(columns.size());
    DBMS dbms = client.server.dbms;
    for (DBMS.Column c: columns) {
      values.add(dbms.parameter());
      columnTypes.add(findColumn(table, c).type);
    }

    return dbms.insertTemplate(table.table, columns, values, false);
  }

  private static Class findColumnType(Server server,
                                      String name)
  {
    Class type = server.columnTypes.get(name);
    if (type == null) {
      throw new RuntimeException("no such column type: " + name);
    } else {
      return type;
    }
  }

  private static Table makeTable(Server server,
                                 Tree tree)
  {
    Tree body = tree.get(4);
    Tree primaryKeyTree = null;
    List<Tree> columns = new ArrayList();
    for (int i = 0; i < body.length(); ++i) {
      Tree item = body.get(i);
      if (item.get(0) instanceof Terminal) {
        if (primaryKeyTree != null) {
          throw new RuntimeException("more than one primary key specified");
        } else {
          primaryKeyTree = item.get(3);
        }
      } else {
        columns.add(item);
      }
    }
    
    if (primaryKeyTree == null) {
      throw new RuntimeException("no primary key specified");
    }

    DBMS dbms = server.dbms;
    Map<String, Column> columnMap = new HashMap(columns.size());
    Set<DBMS.Column> columnSet = new HashSet(columns.size());
    for (Tree column: columns) {
      Class type = findColumnType(server, ((Name) column.get(1)).value);
      DBMS.Column dbmsColumn = dbms.column(type);
      columnSet.add(dbmsColumn);
  
      Column myColumn = new Column
        (((Name) column.get(0)).value, dbmsColumn, type);
      columnMap.put(myColumn.name, myColumn);
    }

    List<Column> myPrimaryKeyColumns = new ArrayList(primaryKeyTree.length());
    List<DBMS.Column> dbmsPrimaryKeyColumns = new ArrayList
      (primaryKeyTree.length());
    for (int i = 0; i < primaryKeyTree.length(); ++i) {
      Column c = columnMap.get(((Name) primaryKeyTree.get(i)).value);
      if (c == null) {
        throw new RuntimeException
          ("primary key refers to non-exisitant column "
           + ((Name) primaryKeyTree.get(i)).value);
      } else {
        myPrimaryKeyColumns.add(c);
        dbmsPrimaryKeyColumns.add(c.column);
      }
    }

    return new Table
      (((Name) tree.get(2)).value, columnMap, myPrimaryKeyColumns,
       dbms.table
       (columnSet, dbms.index(dbmsPrimaryKeyColumns, true), EmptyIndexSet));
  }

  private static void writeInteger(OutputStream out, int v)
    throws IOException
  {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v       ) & 0xFF);
  }

  private static void writeString(OutputStream out,
                                  String string)
    throws IOException
  {
    byte[] bytes = string.getBytes("UTF-8");
    writeInteger(out, bytes.length);
    out.write(bytes);
  }

  public static int readInteger(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();
    if (b4 == -1) throw new EOFException();
    return (int) ((b1 << 24) | (b2 << 16) | (b3 << 8) | (b4));
  }

  public static String readString(InputStream in) throws IOException {
    return new String(new byte[readInteger(in)]);
  }

  private static void diff(DBMS dbms,
                           Revision base,
                           Revision fork,
                           QueryTemplate template,
                           int expressionCount,
                           OutputStream out)
    throws IOException
  {
    QueryResult result = dbms.diff(base, fork, template);

    out.write(Response.RowSet.ordinal());

    while (true) {
      ResultType resultType = result.nextRow();
      switch (resultType) {
      case Inserted:
        out.write(RowSetFlag.InsertedRow.ordinal());
        for (int i = 0; i < expressionCount; ++i) {
          out.write(RowSetFlag.Item.ordinal());
          writeString(out, String.valueOf(result.nextItem()));
        }
        break;

      case Deleted:
        out.write(RowSetFlag.DeletedRow.ordinal());
        for (int i = 0; i < expressionCount; ++i) {
          out.write(RowSetFlag.Item.ordinal());
          writeString(out, String.valueOf(result.nextItem()));
        }
        break;

      case End:
        out.write(RowSetFlag.End.ordinal());
        return;

      default:
        throw new RuntimeException("unexpected result type: " + resultType);
      }
    }
  }

  private static int apply(Client client,
                           PatchTemplate template,
                           Object ... parameters)
  {
    DBMS dbms = client.server.dbms;
    PatchContext context = dbms.patchContext(client.transaction.dbHead);
    int count = dbms.apply(context, template, parameters);
    client.transaction.dbHead = dbms.commit(context);
    return count;
  }

  private static void setDatabase(Client client, Database database) {
    apply(client, client.server.insertOrUpdateDatabase, database.name,
          database);
  }

  private static void setTable(Client client, Table table) {
    apply(client, client.server.insertOrUpdateTable, database(client).name,
          table.name, table);
  }

  private static void setTag(Client client, Tag tag) {
    apply(client, client.server.insertOrUpdateTag, database(client).name,
          tag.name, tag);
  }

  private static void pushTransaction(Client client) {
    Transaction next = client.transaction;
    client.transaction = new Transaction
      (next, next == null ? client.server.dbHead.get() : next.dbHead);
  }

  private static void commitTransaction(Client client) {
    if (client.transaction.next == null) {
      Revision myTail = client.transaction.dbTail;
      Revision myHead = client.transaction.dbHead;
      DBMS dbms = client.server.dbms;
      DBMS.ConflictResolver conflictResolver = client.server.conflictResolver;
      if (myTail != myHead) {
        AtomicReference<Revision> dbHead = client.server.dbHead;
        while (! dbHead.compareAndSet(myTail, myHead)) {
          Revision fork = dbHead.get();
          myHead = dbms.merge(myTail, fork, myHead, conflictResolver);
          myTail = fork;
        }
      }
    } else {
      client.transaction.next.dbHead = client.transaction.dbHead;
    }
  }

  private static void popTransaction(Client client) {
    client.transaction = client.transaction.next;
  }

  private static int apply(Client client,
                           PatchTemplate template)
  {
    pushTransaction(client);
    try {
      DBMS dbms = client.server.dbms;
      PatchContext context = dbms.patchContext(head(client));
      int count = dbms.apply(context, template);
      setTag(client, new Tag("head", dbms.commit(context)));
      commitTransaction(client);
      return count;
    } finally {
      popTransaction(client);
    }
  }

  private static Object convert(Class type,
                                String value)
  {
    if (type == Integer.class) {
      return Integer.parseInt(value);
    } else if (type == Long.class) {
      return Long.parseLong(value);
    } else if (type == String.class) {
      return value;
    } else {
      throw new RuntimeException("unexpected type: " + type);
    }
  }

  private static int copy(InputStream in,
                          DBMS dbms,
                          PatchContext context,
                          PatchTemplate template,
                          List<Class> columnTypes)
    throws IOException
  {
    StringBuilder sb = new StringBuilder();
    Object[] parameters = new Object[columnTypes.size()];
    boolean sawEscape = false;
    int i = 0;
    int count = 0;
    while (true) {
      int c = in.read();
      switch (c) {
      case -1:
        throw new EOFException();

      case '\\':
        if (sawEscape) {
          sb.append((char) c);
        } else {
          sawEscape = true;
        }
        break;

      case '\t':
        if (sawEscape) {
          sb.append((char) c);
        } else {
          parameters[i] = convert(columnTypes.get(i), sb.toString());
          sb.setLength(0);
          ++ i;
        }
        break;

      case '\n':
        if (sawEscape) {
          sb.append((char) c);
        } else {
          parameters[i] = convert(columnTypes.get(i), sb.toString());
          sb.setLength(0);
          if (i < parameters.length - 1) {
            throw new RuntimeException("not enough values specified");
          }
          dbms.apply(context, template, parameters);
          ++ count;
          i = 0;
        }
        break;

      case '.':
        if (sawEscape) {
          if (i != 0 || sb.length() > 0) {
            throw new RuntimeException("unexpected end of values");
          }
          return count;
        } else {
          sb.append((char) c);
        }
        break;

      default:
        sb.append((char) c);
        break;
      }
    }
  }

  private static int applyCopy(Client client,
                               PatchTemplate template,
                               List<Class> columnTypes,
                               InputStream in)
    throws IOException
  {
    pushTransaction(client);
    try {
      DBMS dbms = client.server.dbms;
      PatchContext context = dbms.patchContext(head(client));
      int count = copy(in, dbms, context, template, columnTypes);
      setTag(client, new Tag("head", dbms.commit(context)));
      commitTransaction(client);
      return count;
    } finally {
      popTransaction(client);
    }
  }

  private static void addCompletion(ParseContext context,
                                    NameType type,
                                    String name)
  {
    List<String> list = context.completions.get(type);
    if (list == null) {
      context.completions.put(type, list = new ArrayList());
    }
    list.add(name);
  }

  private static List<String> findCompletions(Client client,
                                              NameType type)
  {
    DBMS dbms = client.server.dbms;
    QueryResult result = null;
    switch (type) {
    case Database:
      result = dbms.diff
        (dbms.revision(), dbHead(client), client.server.listDatabases);
      break;

    case Table:
      if (client.database != null) {
        result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTables,
           client.database);
      }
      break;

    case Column:
      break;

    case Tag:
      if (client.database != null) {
        result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTags,
           client.database);
      }
      break;
      
    default: throw new RuntimeException("unexpected name type: " + type);
    }
      
    List<String> list = null;
    if (result != null) {
      while (result.nextRow() == ResultType.Inserted) {
        if (list == null) {
          list = new ArrayList();
        }
        list.add((String) result.nextItem());
      }
    }
    return list;
  }

  private static List<String> findCompletions(ParseContext context,
                                              NameType type)
  {
    if (type == NameType.Column) {
      List<String> columnList = context.completions.get(type);
      if (columnList != null) {
        return columnList;
      } else {
        List<String> tableList = context.completions.get(type);
        if (tableList != null && context.client.database != null) {
          DBMS dbms = context.client.server.dbms;
          Revision tail = dbms.revision();
          Revision head = dbHead(context.client);
          Set<String> columns = new HashSet();
          for (String tableName: tableList) {
            QueryResult result = dbms.diff
              (tail, head, context.client.server.findTable,
               context.client.database.name, tableName);

            if (result.nextRow() == ResultType.Inserted) {
              for (Column c: ((Table) result.nextItem()).columns.values()) {
                columns.add(c.name);
              }
            }          
          }

          if (columns.size() == 0) {
            return null;
          } else {
            return new ArrayList(columns);
          }
        } else {
          return null;
        }
      }
    } else {
      return findCompletions(context.client, type);
    }
  }

  private static class ParserFactory {
    public LazyParser expressionParser;
    public LazyParser sourceParser;

    public Parser parser() {
      return or
        (select(),
         diff(),
         insert(),
         update(),
         delete(),
         copy(),
         begin(),
         commit(),
         rollback(),
         tag(),
         merge(),
         list(),
         drop(),
         useDatabase(),
         createTable(),
         createDatabase(),
         help());
    }

    public static Parser task(final Parser parser, final Task task) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          ParseResult result = parser.parse(context, in);
          if (result.tree != null) {
            result.task = task;
          }
          return result;
        }
      };
    }

    public static ParseResult success(Tree tree, String next) {
      return new ParseResult(tree, next, null);
    }

    public static ParseResult fail(List<String> completions) {
      return new ParseResult(null, null, completions);
    }

    public static String next(String in, int offset) {
      int i = offset;
      while (i < in.length()) {
        if (Character.isSpace(in.charAt(i))) {
          ++ i;
        } else {
          break;
        }
      }

      return in.substring(i);
    }

    public static String parseName(String in) {
      if (in.length() > 0) {
        char c = in.charAt(0);
        if (c == '_' || Character.isLetter(c)) {
          int i = 1;
          while (i < in.length()) {
            c = in.charAt(i);
            if (c == '_' || Character.isLetterOrDigit(c)) {
              ++ i;
            } else {
              break;
            }
          }
          return in.substring(0, i);
        }
      }
      return null;
    }

    public static Parser terminal(final String value) {
      return new Parser() {
        private final Terminal terminal = new Terminal(value);

        public ParseResult parse(ParseContext context, String in) {
          if (in.startsWith(value)) {
            return success(terminal, next(in, value.length()));
          } else if (value.startsWith(in)) {
            return fail(Util.list(value));
          } else {
            return fail(null);
          }
        }
      };
    }

    public static Parser or(final Parser ... parsers) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          List<String> completions = null;
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in);
            if (result.tree != null) {
              return result;
            } else if (result.completions != null) {
              if (completions == null) {
                completions = new ArrayList();
              }
              completions.addAll(result.completions);
            }
          }
          return fail(completions);
        }
      };
    }

    public static Parser list(final Parser parser) {
      return new Parser() {
        private final Parser comma = terminal(",");

        public ParseResult parse(ParseContext context, String in) {
          TreeList list = new TreeList();
          while (true) {
            ParseResult result = parser.parse(context, in);
            if (result.tree != null) {
              list.add(result.tree);

              ParseResult commaResult = comma.parse(context, result.next);
              if (commaResult.tree != null) {
                in = commaResult.next;
              } else {
                return success(list, result.next);
              }
            } else {
              return fail(result.completions);
            }
          }
        }
      };
    }

    public static Parser sequence(final Parser ... parsers) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          TreeList list = new TreeList();
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in);
            if (result.tree != null) {
              list.add(result.tree);
              in = result.next;
            } else {
              return fail(result.completions);
            }
          }
          return success(list, in);
        }
      };
    }

    public static Parser optional(final Parser parser) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          ParseResult result = parser.parse(context, in);
          if (result.tree != null) {
            return result;
          } else {
            return success(Nothing, in);
          }
        }
      };
    }

    public static Parser name(final NameType type,
                              final boolean findCompletions,
                              final boolean addCompletion)
    {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          String name = parseName(in);
          if (name != null) {
            if (addCompletion) {
              addCompletion(context, type, name);
            }
            return success(new Name(name), next(in, name.length()));
          } else {
            return fail
              (findCompletions ? findCompletions(context, type) : null);
          }
        }
      };
    }

    public static Parser stringLiteral() {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          if (in.length() > 1) {
            StringBuilder sb = new StringBuilder();
            char c = in.charAt(0);
            if (c == '\'') {
              int i = 1;
              boolean sawEscape = false;
              while (i < in.length()) {
                c = in.charAt(i);
                switch (c) {
                case '\\':
                  if (sawEscape) {
                    sawEscape = false;
                    sb.append(c);
                  } else {
                    sawEscape = true;
                  }
                  break;

                case '\'':
                  if (sawEscape) {
                    sawEscape = false;
                    sb.append(c);
                  } else {
                    return success(new StringLiteral(sb.toString()),
                                   next(in, i + 1));
                  }
                  break;

                default:
                  if (sawEscape) {
                    sawEscape = false;
                    sb.append('\\');
                  }
                  sb.append(c);
                  break;
                }
              }
            }
          }
          return fail(null);
        }
      };
    }

    public static Parser numberLiteral() {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          if (in.length() > 0) {
            char first = in.charAt(0);
            boolean negative = first == '-';
            int i = negative ? 1 : 0;
            while (i < in.length()) {
              char c = in.charAt(i);
              if (Character.isDigit(c)) {
                break;
              } else {
                if (i > 0) {
                  success(new NumberLiteral(in.substring(0, i)),
                          next(in, i));
                } else {
                  fail(null);
                }
              }
            }
          }
          return fail(null);
        }
      };
    }

    public Parser expression() {
      if (expressionParser == null) {
        expressionParser = new LazyParser();
        expressionParser.parser = or
          (columnName(),
           stringLiteral(),
           numberLiteral(),
           sequence(terminal("("),
                    expression(),
                    terminal(")")),
           sequence(terminal("not"),
                    expression()),
           sequence(expression(),
                    or(terminal("and"),
                       terminal("or"),
                       terminal("="),
                       terminal("<>"),
                       terminal("<"),
                       terminal("<="),
                       terminal(">"),
                       terminal(">=")),
                    expression()));
      }

      return expressionParser;
    }

    public Parser source() {
      if (sourceParser == null) {
        sourceParser = new LazyParser();
        sourceParser.parser = or
          (name(NameType.Table, true, true),
           sequence(terminal("("),
                    source(),
                    terminal(")")),
           sequence(source(),
                    sequence(or(terminal("left"),
                                terminal("inner")),
                             terminal("join")),
                    source(),
                    terminal("on"),
                    expression()));
      }

      return sourceParser;
    }

    public static Parser columnName() {
      return or
        (name(NameType.Column, true, false),
         sequence(name(NameType.Table, true, false),
                  terminal("."),
                  name(NameType.Column, true, false)));
    }

    public Parser select() {
      return task
        (sequence
         (terminal("select"),
          or(terminal("*"), list(expression())),
          terminal("from"),
          source(),
          optional(sequence(terminal("where"),
                            expression()))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             DBMS dbms = client.server.dbms;
             int[] expressionCount = new int[1];
             SQLServer.diff
               (dbms, dbms.revision(), head(client), makeQueryTemplate
                (client, tree, expressionCount), expressionCount[0], out);
           }           
         });
    }

    public Parser diff() {
      return task
        (sequence
         (terminal("diff"),
          name(NameType.Tag, true, false),
          name(NameType.Tag, true, false),
          select()),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             DBMS dbms = client.server.dbms;
             int[] expressionCount = new int[1];
             SQLServer.diff
               (dbms, findTag(client, ((Name) tree.get(1)).value).revision,
                findTag(client, ((Name) tree.get(2)).value).revision,
                makeQueryTemplate
                (client, tree.get(3), expressionCount), expressionCount[0],
                out);
           }           
         });
    }

    public Parser insert() {
      return task
        (sequence
         (terminal("insert"),
          terminal("into"),
          name(NameType.Table, true, true),
          optional(sequence(terminal("("),
                            list(columnName()),
                            terminal(")"))),
          terminal("values"),
          terminal("("),
          list(expression()),
          terminal(")")),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             apply(client, makeInsertTemplate(client, tree));

             out.write(Response.Success.ordinal());
             writeString(out, "inserted 1 row");
           }
         });
    }

    public Parser update() {
      return task
        (sequence
         (terminal("update"),
          name(NameType.Table, true, true),
          terminal("set"),
          list(sequence(columnName(),
                        terminal("="),
                        expression())),
          optional(sequence(terminal("where"),
                            expression()))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             int count = apply(client, makeUpdateTemplate(client, tree));

             out.write(Response.Success.ordinal());
             writeString(out, "updated " + count + " row(s)");
           }
         });
    }

    public Parser delete() {
      return task
        (sequence
         (terminal("delete"),
          terminal("from"),
          name(NameType.Table, true, true),
          optional(sequence(terminal("where"),
                            expression()))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             int count = apply(client, makeDeleteTemplate(client, tree));

             out.write(Response.Success.ordinal());
             writeString(out, "deleted " + count + " row(s)");
           }
         });
    }

    public Parser copy() {
      return task
        (sequence
         (terminal("copy"),
          name(NameType.Table, true, true),
          optional(sequence(terminal("("),
                            list(columnName()),
                            terminal(")"))),
          terminal("from"),
          terminal("stdin")),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             List<Class> columnTypes = new ArrayList();
             int count = applyCopy
               (client, makeCopyTemplate(client, tree, columnTypes),
                columnTypes, in);

             out.write(Response.Success.ordinal());
             writeString(out, "inserted " + count + " row(s)");
           }
         });
    }

    public static Parser begin() {
      return task
        (terminal("begin"),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             pushTransaction(client);

             out.write(Response.Success.ordinal());
             writeString(out, "pushed new transaction context");
           }
         });
    }

    public static Parser commit() {
      return task
        (terminal("commit"),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             commitTransaction(client);
             popTransaction(client);

             out.write(Response.Success.ordinal());
             writeString(out, "committed transaction");
           }
         });
    }

    public static Parser rollback() {
      return task
        (terminal("rollback"),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             popTransaction(client);

             out.write(Response.Success.ordinal());
             writeString(out, "abandoned transaction");
           }
         });
    }

    public static Parser tag() {
      return task
        (sequence
         (terminal("tag"),
          name(NameType.Tag, true, false),
          name(NameType.Tag, false, false)),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             pushTransaction(client);
             try {
               setTag(client, new Tag
                      (((Name) tree.get(1)).value,
                       findTag(client, ((Name) tree.get(2)).value).revision));
               commitTransaction(client);
             } finally {
               popTransaction(client);
             }

             out.write(Response.Success.ordinal());
             writeString(out, "tag " + ((Name) tree.get(1)).value
                         + " set to " + ((Name) tree.get(2)).value);
           }
         });
    }

    public static Parser merge() {
      return task
        (sequence
         (terminal("merge"),
          name(NameType.Tag, true, false),
          name(NameType.Tag, true, false),
          name(NameType.Tag, true, false)),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             LeftPreferenceConflictResolver conflictResolver
               = new LeftPreferenceConflictResolver();
             pushTransaction(client);
             try {
               setTag(client, new Tag
                      ("head",
                       client.server.dbms.merge
                       (findTag(client, ((Name) tree.get(1)).value).revision,
                        findTag(client, ((Name) tree.get(2)).value).revision,
                        findTag(client, ((Name) tree.get(3)).value).revision,
                        conflictResolver)));
               commitTransaction(client);
             } finally {
               popTransaction(client);
             }

             out.write(Response.Success.ordinal());
             writeString(out, "head set to result of merge ("
                         + conflictResolver.conflictCount + " conflicts)");
           }
         });
    }

    public static Parser list() {
      return task
        (sequence
         (terminal("list"),
          or(terminal("databases"),
             terminal("tables"),
             terminal("tags"),
             sequence(terminal("columns"),
                      terminal("of"),
                      name(NameType.Table, true, false)))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             out.write(Response.Success.ordinal());
             writeString(out, "todo");
           }
         });
    }

    public static Parser drop() {
      return task
        (sequence
         (terminal("drop"),
          or(sequence(terminal("database"),
                      name(NameType.Database, true, false)),
             sequence(terminal("table"),
                      name(NameType.Table, true, false)),
             sequence(terminal("tag"),
                      name(NameType.Tag, true, false)))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             tree = tree.get(1);
             String type = ((Terminal) tree.get(0)).value;
             String name = ((Name) tree.get(1)).value;

             pushTransaction(client);
             try {
               DBMS dbms = client.server.dbms;
               PatchContext context = dbms.patchContext
                 (client.transaction.dbHead);

               if ("database".equals(type)) {
                 dbms.apply(context, client.server.deleteDatabase, name);
                 dbms.apply(context, client.server.deleteDatabaseTables, name);
                 dbms.apply(context, client.server.deleteDatabaseTags, name);
               } else if ("table".equals(type)) {
                 dbms.apply(context, client.server.deleteTable,
                            database(client), name);
               } else if ("tag".equals(type)) {
                 dbms.apply(context, client.server.deleteTag,
                            database(client), name);
               } else {
                 throw new RuntimeException();
               }

               client.transaction.dbHead = dbms.commit(context);

               commitTransaction(client);
             } finally {
               popTransaction(client);
             }

             out.write(Response.Success.ordinal());
             writeString(out, "dropped " + type + " " + name);
           }
         });
    }

    public static Parser useDatabase() {
      return task
        (sequence
         (terminal("use"),
          terminal("database"),
          name(NameType.Database, true, false)),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             String name = ((Name) tree.get(2)).value;
             client.database = findDatabase(client, name);

             out.write(Response.Success.ordinal());
             writeString(out, "switched to database " + name);
           }
         });
    }

    public static Parser createTable() {
      return task
        (sequence
         (terminal("create"),
          terminal("table"),
          name(NameType.Table, false, false),
          terminal("("),
          list(or(sequence(terminal("primary"),
                           terminal("key"),
                           terminal("("),
                           list(name(NameType.Column, true, false)),
                           terminal(")")),
                  sequence(name(NameType.Column, false, true),
                           or(terminal("int32"),
                              terminal("int64"),
                              terminal("string"),
                              terminal("array"))))),
          terminal(")")),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             pushTransaction(client);
             try {
               setTable(client, makeTable(client.server, tree));
               commitTransaction(client);
             } finally {
               popTransaction(client);
             }

             out.write(Response.Success.ordinal());
             writeString(out, "table " + ((Name) tree.get(2)).value
                         + " defined");
           }
         });
    }

    public static Parser createDatabase() {
      return task
        (sequence
         (terminal("create"),
          terminal("database"),
          name(NameType.Database, false, false)),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             String name = ((Name) tree.get(2)).value;
             pushTransaction(client);
             try {
               setDatabase(client, new Database(name));
               commitTransaction(client);
             } finally {
               popTransaction(client);
             }

             out.write(Response.Success.ordinal());
             writeString(out, "created database " + name);
           }
         });
    }

    public static Parser help() {
      return task
        (terminal("help"),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             out.write(Response.Success.ordinal());
             writeString(out, "todo");
           }
         });
    }
  }

  private static void executeRequest(Client client,
                                     InputStream in,
                                     OutputStream out)
    throws IOException
  {
    ParseResult result = client.server.parser.parse
      (new ParseContext(client), readString(in));
    if (result.task != null) {
      try {
        result.task.run(client, result.tree, in, out);
      } catch (Exception e) {
        out.write(Response.Error.ordinal());
        writeString(out, e.getMessage()); 
        log.log(Level.WARNING, null, e);       
      }
    } else {
      out.write(Response.Error.ordinal());
      writeString(out, "Sorry, I don't understand.");
    }
  }

  private static void completeRequest(Client client,
                                      InputStream in,
                                      OutputStream out)
    throws IOException
  {
    ParseResult result = client.server.parser.parse
      (new ParseContext(client), readString(in));
    out.write(Response.Success.ordinal());
    writeInteger(out, result.completions.size());
    for (String completion: result.completions) {
      writeString(out, completion);
    }
  }

  private static void handleRequest(Client client,
                                    InputStream in,
                                    OutputStream out)
    throws IOException
  {
    int requestType = in.read();
    switch (Request.values()[requestType]) {
    case Execute:
      executeRequest(client, in, out);
      out.flush();
      break;

    case Complete:
      completeRequest(client, in, out);
      out.flush();
      break;

    default:
      throw new RuntimeException("unexpected request type: " + requestType);
    }
  }

  private static void listen(String address,
                             int port)
    throws IOException
  {
    ServerSocketChannel serverChannel = ServerSocketChannel.open();
    serverChannel.socket().bind(new InetSocketAddress(address, port));

    ThreadPoolExecutor executor = new ThreadPoolExecutor
      (ThreadPoolSize,       // core thread count
       ThreadPoolSize,       // maximum thread count
       60, TimeUnit.SECONDS, // maximum thread idle time
       new LinkedBlockingQueue<Runnable>());

    executor.allowCoreThreadTimeOut(true);

    Server server = new Server(new MyDBMS());

    while (true) {
      executor.execute(new Client(server, serverChannel.accept()));
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 2) {
      listen(args[0], Integer.parseInt(args[1]));
    } else {
      System.err.println("usage: java " + SQLServer.class.getName()
                         + " <address> <port>");
      System.exit(-1);
    }
  }
}
