package com.readytalk.oss.dbms.server;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.set;

import com.readytalk.oss.dbms.util.Util;
import com.readytalk.oss.dbms.imp.MyDBMS;
import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Index;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.Source;
import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.QueryResult;
import com.readytalk.oss.dbms.ConflictResolver;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Join;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.UnaryOperation;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.DuplicateKeyResolution;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.QueryTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.UpdateTemplate;

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
import java.io.Reader;
import java.io.InputStreamReader;
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
  private static final boolean Debug = true;

  private static final Logger log = Logger.getLogger("SQLServer");

  public enum Request {
    Execute, Complete;
  }

  public enum Response {
    RowSet, NewDatabase, CopySuccess, Success, Error;
  }

  public enum RowSetFlag {
    InsertedRow, DeletedRow, End, Item;
  }

  private static final int ThreadPoolSize = 256;

  private static final Tree Nothing = new Leaf();

  private static int nextId = 1;

  private synchronized static String makeId() {
    return SQLServer.class.getName() + ".name" + (nextId++);
  }

  private static class Server {
    public final DBMS dbms;

    public final Parser parser = new ParserFactory().parser();

    public final PatchTemplate insertOrUpdateDatabase;
    public final QueryTemplate listDatabases;
    public final QueryTemplate findDatabase;
    public final PatchTemplate deleteDatabase;

    public final PatchTemplate insertOrUpdateTable;
    public final QueryTemplate listTables;
    public final QueryTemplate findTable;
    public final PatchTemplate deleteDatabaseTables;
    public final PatchTemplate deleteTable;

    public final Column tagsDatabase;
    public final Column tagsName;
    public final Column tagsTag;
    public final Table tags;

    public final PatchTemplate insertOrUpdateTag;
    public final QueryTemplate listTags;
    public final QueryTemplate findTag;
    public final PatchTemplate deleteDatabaseTags;
    public final PatchTemplate deleteTag;

    public final AtomicReference<Revision> dbHead;
    public final Map<String, BinaryOperation.Type> binaryOperationTypes
      = new HashMap();
    public final Map<String, UnaryOperation.Type> unaryOperationTypes
      = new HashMap();
    public final Map<String, Class> columnTypes = new HashMap();
    public final ConflictResolver rightPreferenceConflictResolver
      = new ConflictResolver() {
          public Object resolveConflict(Table table,
                                        Column column,
                                        Object[] primaryKeyValues,
                                        Object baseValue,
                                        Object leftValue,
                                        Object rightValue)
          {
            return rightValue;
          }
        };
    public final ConflictResolver conflictResolver = new ConflictResolver() {
        public Object resolveConflict(Table table,
                                      Column column,
                                      Object[] primaryKeyValues,
                                      Object baseValue,
                                      Object leftValue,
                                      Object rightValue)
        {
          if (table == tags) {
            expect(column == tagsTag);

            return new Tag
              (((Tag) leftValue).name, dbms.merge
               (baseValue == null
                ? dbms.revision() : ((Tag) baseValue).revision,
                ((Tag) leftValue).revision,
                ((Tag) rightValue).revision,
                rightPreferenceConflictResolver));
          } else {
            return rightValue;
          }
        }
      };

    public Server(DBMS dbms) {
      this.dbms = dbms;

      Column databasesName = new Column(String.class, makeId());
      Column databasesDatabase = new Column(Object.class, makeId());
      Table databases = new Table(list(databasesName), makeId());
      TableReference databasesReference = new TableReference(databases);

      this.insertOrUpdateDatabase = new InsertTemplate
        (databases, list(databasesName, databasesDatabase),
         list((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listDatabases = new QueryTemplate
        (list((Expression) new ColumnReference
              (databasesReference, databasesDatabase)),
         databasesReference,
         new Constant(true));

      this.findDatabase = new QueryTemplate
        (list((Expression) new ColumnReference
              (databasesReference, databasesDatabase)),
         databasesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(databasesReference, databasesName),
          new Parameter()));

      this.deleteDatabase = new DeleteTemplate
        (databasesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(databasesReference, databasesName),
          new Parameter()));

      Column tablesDatabase = new Column(String.class, makeId());
      Column tablesName = new Column(String.class, makeId());
      Column tablesTable = new Column(Object.class, makeId());
      Table tables = new Table(list(tablesDatabase, tablesName), makeId());
      TableReference tablesReference = new TableReference(tables);

      this.insertOrUpdateTable = new InsertTemplate
        (tables, list(tablesDatabase, tablesName, tablesTable),
         list((Expression) new Parameter(), new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listTables = new QueryTemplate
        (list((Expression) new ColumnReference(tablesReference, tablesTable)),
         tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(tablesReference, tablesDatabase),
          new Parameter()));

      this.findTable = new QueryTemplate
        (list((Expression) new ColumnReference(tablesReference, tablesTable)),
         tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tablesReference, tablesDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tablesReference, tablesName),
           new Parameter())));

      this.deleteDatabaseTables = new DeleteTemplate
        (tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(tablesReference, tablesDatabase),
          new Parameter()));

      this.deleteTable = new DeleteTemplate
        (tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tablesReference, tablesDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tablesReference, tablesName),
           new Parameter())));

      this.tagsDatabase = new Column(String.class, makeId());
      this.tagsName = new Column(String.class, makeId());
      this.tagsTag = new Column(Object.class, makeId());
      this.tags = new Table(list(tagsDatabase, tagsName), makeId());
      TableReference tagsReference = new TableReference(tags);

      this.insertOrUpdateTag = new InsertTemplate
        (tags, list(tagsDatabase, tagsName, tagsTag),
         list((Expression) new Parameter(), new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listTags = new QueryTemplate
        (list((Expression) new ColumnReference(tagsReference, tagsTag)),
         tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(tagsReference, tagsDatabase),
          new Parameter()));

      this.findTag = new QueryTemplate
        (list((Expression) new ColumnReference(tagsReference, tagsTag)),
         tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tagsReference, tagsDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tagsReference, tagsName),
           new Parameter())));

      this.deleteDatabaseTags = new DeleteTemplate
        (tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          new ColumnReference(tagsReference, tagsDatabase),
          new Parameter()));

      this.deleteTag = new DeleteTemplate
        (tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tagsReference, tagsDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           new ColumnReference(tagsReference, tagsName),
           new Parameter())));

      this.dbHead = new AtomicReference(dbms.revision());

      binaryOperationTypes.put("and", BinaryOperation.Type.And);
      binaryOperationTypes.put("or", BinaryOperation.Type.Or);
      binaryOperationTypes.put("=", BinaryOperation.Type.Equal);
      binaryOperationTypes.put("<>", BinaryOperation.Type.NotEqual);
      binaryOperationTypes.put(">", BinaryOperation.Type.GreaterThan);
      binaryOperationTypes.put(">=", BinaryOperation.Type.GreaterThanOrEqual);
      binaryOperationTypes.put("<", BinaryOperation.Type.LessThan);
      binaryOperationTypes.put("<=", BinaryOperation.Type.LessThanOrEqual);

      unaryOperationTypes.put("not", UnaryOperation.Type.Not);

      columnTypes.put("int32", Integer.class);
      columnTypes.put("int64", Long.class);
      columnTypes.put("string", String.class);
    }
  }

  private static void expect(boolean v) {
    if (Debug && ! v) {
      throw new RuntimeException();
    }
  }

  private static boolean equal(Object left,
                               Object right)
  {
    return left == right || (left != null && left.equals(right));
  }

  private static class LeftPreferenceConflictResolver
    implements ConflictResolver
  {
    public int conflictCount;

    public Object resolveConflict(Table table,
                                  Column column,
                                  Object[] primaryKeyValues,
                                  Object baseValue,
                                  Object leftValue,
                                  Object rightValue)
    {
      ++ conflictCount;
      return leftValue;
    }
  }

  private static class Transaction {
    public final Transaction next;
    public final Revision dbTail;
    public Revision dbHead;

    public Transaction(Transaction next,
                       Revision dbTail)
    {
      this.next = next;
      this.dbTail = dbTail;
      this.dbHead = dbTail;
    }
  }

  private static class CopyContext {
    public final RevisionBuilder builder;
    public final PatchTemplate template;
    public final List<Class> columnTypes;
    public final StringBuilder stringBuilder = new StringBuilder();
    public final Object[] parameters;
    public int count;
    public boolean trouble;

    public CopyContext(RevisionBuilder builder,
                       PatchTemplate template,
                       List<Class> columnTypes)
    {
      this.builder = builder;
      this.template = template;
      this.columnTypes = columnTypes;
      this.parameters = new Object[columnTypes.size()];
    }
  }

  private static class Client implements Runnable {
    public final Server server;
    public final SocketChannel channel;
    public Transaction transaction;
    public Database database;
    public CopyContext copyContext;

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
    public final String name;

    public Database(String name) {
      this.name = name;
    }
  }

  private static class MyColumn {
    public final String name;
    public final Column column;
    public final Class type;
    
    public MyColumn(String name,
                  Column column,
                  Class type)
    {
      this.name = name;
      this.column = column;
      this.type = type;
    }
  }

  private static class MyTable {
    public final String name;
    public final List<MyColumn> columnList;
    public final Map<String, MyColumn> columnMap;
    public final List<MyColumn> primaryKeyColumns;
    public final Table table;

    public MyTable(String name,
                 List<MyColumn> columnList,
                 Map<String, MyColumn> columnMap,
                 List<MyColumn> primaryKeyColumns,
                 Table table)
    {
      this.name = name;
      this.columnList = columnList;
      this.columnMap = columnMap;
      this.primaryKeyColumns = primaryKeyColumns;
      this.table = table;
    }
  }

  private static class Tag {
    public final String name;
    public final Revision revision;

    public Tag(String name,
               Revision revision)
    {
      this.name = name;
      this.revision = revision;
    }
  }
  
  private static class MyTableReference {
    public final MyTable table;
    public final TableReference reference;

    public MyTableReference(MyTable table,
                            TableReference reference)
    {
      this.table = table;
      this.reference = reference;
    }
  }

  private static class ParseResult {
    public final Tree tree;
    public final String next;
    public final Set<String> completions;
    public Task task;

    public ParseResult(Tree tree,
                       String next,
                       Set<String> completions)
    {
      this.tree = tree;
      this.next = next;
      this.completions = completions;
    }
  }

  private static class ParseContext {
    public final Client client;
    public final String start;
    public Map<NameType, Set<String>> completions;
    public int depth;

    public ParseContext(Client client,
                        String start)
    {
      this.client = client;
      this.start = start;
    }
  }

  private interface Parser {
    public ParseResult parse(ParseContext context, String in);
  }

  private static class LazyParser implements Parser {
    public Parser parser;

    public ParseResult parse(ParseContext context, String in) {
      if (context.depth++ > 10) throw new RuntimeException("debug");

      return parser.parse(context, in);
    }
  }

  private interface Tree {
    public Tree get(int index);
    public int length();
  }

  private static class TreeList implements Tree {
    public final List<Tree> list = new ArrayList();
    
    public void add(Tree tree) {
      list.add(tree);
    }

    public Tree get(int index) {
      return list.get(index);
    }

    public int length() {
      return list.size();
    }

    public String toString() {
      return list.toString();
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

    public String toString() {
      return "terminal[" + value + "]";
    }
  }

  private static class Name extends Leaf {
    public final String value;

    public Name(String value) {
      this.value = value;
    }

    public String toString() {
      return "name[" + value + "]";
    }
  }

  private static class StringLiteral extends Leaf {
    public final String value;

    public StringLiteral(String value) {
      this.value = value;
    }

    public String toString() {
      return "stringLiteral[" + value + "]";
    }
  }

  private static class NumberLiteral extends Leaf {
    public final long value;

    public NumberLiteral(String value) {
      this.value = Long.parseLong(value);
    }

    public String toString() {
      return "numberLiteral[" + value + "]";
    }
  }

  private static class BooleanLiteral extends Leaf {
    public final boolean value;

    public BooleanLiteral(boolean value) {
      this.value = value;
    }

    public String toString() {
      return "booleanLiteral[" + value + "]";
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

    if (result.nextRow() == QueryResult.Type.Inserted) {
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

    if ("tail".equals(name)) {
      return new Tag(name, dbms.revision());
    }

    QueryResult result = dbms.diff
      (dbms.revision(), dbHead(client), server.findTag, database(client).name,
       name);

    if (result.nextRow() == QueryResult.Type.Inserted) {
      return (Tag) result.nextItem();
    } else if ("head".equals(name)) {
      return new Tag(name, dbms.revision());
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

  private static MyTable findTable(Client client,
                                   String name)
  {
    DBMS dbms = client.server.dbms;
    QueryResult result = dbms.diff
      (dbms.revision(), dbHead(client), client.server.findTable,
       database(client).name, name);

    if (result.nextRow() == QueryResult.Type.Inserted) {
      return (MyTable) result.nextItem();
    } else {
      throw new RuntimeException("no such table: " + name);
    }
  }

  private static Source makeSource(Client client,
                                   Tree tree,
                                   List<MyTableReference> tableReferences,
                                   List<Expression> tests)
  {
    if (tree instanceof Name) {
      MyTable table = findTable(client, ((Name) tree).value);
      TableReference reference = new TableReference(table.table);
      tableReferences.add(new MyTableReference(table, reference));
      return reference;
    } else if (tree.get(0) instanceof Terminal) {
      return makeSource(client, tree.get(1), tableReferences, tests);
    } else {
      Source result = new Join
        ("left".equals(((Terminal) tree.get(1).get(0)).value)
         ? Join.Type.LeftOuter : Join.Type.Inner,
         makeSource(client, tree.get(0), tableReferences, tests),
         makeSource(client, tree.get(2), tableReferences, tests));

      tests.add(makeExpression(client.server, tree.get(4), tableReferences));

      return result;
    }
  }

  private static ColumnReference makeColumnReference
    (DBMS dbms,
     List<MyTableReference> tableReferences,
     String tableName,
     String columnName)
  {
    TableReference reference = null;
    Column column = null;
    for (MyTableReference r: tableReferences) {
      if (tableName == null || tableName.equals(r.table.name)) {
        MyColumn c = r.table.columnMap.get(columnName);
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
      throw new RuntimeException
        ("no such column: " + (tableName == null ? "" : tableName + ".")
         + columnName);
    }

    return new ColumnReference(reference, column);
  }

  private static UnaryOperation.Type findUnaryOperationType(Server server,
                                                           String name)
  {
    return server.unaryOperationTypes.get(name);
  }

  private static BinaryOperation.Type findBinaryOperationType(Server server,
                                                             String name)
  {
    return server.binaryOperationTypes.get(name);
  }

  private static Expression makeExpression
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    if (tree instanceof Name) {
      return makeColumnReference
        (server.dbms, tableReferences, null, ((Name) tree).value);
    } else if (tree instanceof StringLiteral) {
      return new Constant(((StringLiteral) tree).value);
    } else if (tree instanceof NumberLiteral) {
      return new Constant(((NumberLiteral) tree).value);
    } else if (tree instanceof BooleanLiteral) {
      return new Constant(((BooleanLiteral) tree).value);
    } if (tree.length() == 3) {
      if (tree.get(0) instanceof Name
          && ".".equals(((Terminal) tree.get(1)).value))
      {
        return makeColumnReference
          (server.dbms,
           tableReferences,
           ((Name) tree.get(0)).value,
           ((Name) tree.get(2)).value);
      }
      return new BinaryOperation
        (findBinaryOperationType(server, ((Terminal) tree.get(1)).value),
         makeExpression(server, tree.get(0), tableReferences),
         makeExpression(server, tree.get(2), tableReferences));      
    } else {
      String value = ((Terminal) tree.get(0)).value;
      if ("(".equals(value)) {
        return makeExpression(server, tree.get(1), tableReferences);
      } else {
        return new UnaryOperation
          (findUnaryOperationType(server, value), makeExpression
           (server, tree.get(1), tableReferences));
      }
    }
  }

  private static List<Expression> makeExpressionList
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    List<Expression> expressions = new ArrayList();
    if (tree instanceof Terminal) {
      for (MyTableReference tableReference: tableReferences) {
        for (MyColumn column: tableReference.table.columnList) {
          expressions.add
            (new ColumnReference(tableReference.reference, column.column));
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
     List<MyTableReference> tableReferences)
  {
    if (tree == Nothing) {
      return new Constant(true);
    } else {
      return makeExpression(server, tree.get(1), tableReferences);
    }
  }

  private static Expression andExpressions(DBMS dbms,
                                           Expression expression,
                                           List<Expression> expressions)
  {
    for (Expression e: expressions) {
      expression = new BinaryOperation
        (BinaryOperation.Type.And, expression, e);
    }
    return expression;
  }

  private static QueryTemplate makeQueryTemplate(Client client,
                                                 Tree tree,
                                                 int[] expressionCount)
  {
    List<MyTableReference> tableReferences = new ArrayList();
    List<Expression> tests = new ArrayList();
    Source source = makeSource
      (client, tree.get(3), tableReferences, tests);

    List<Expression> expressions = makeExpressionList
      (client.server, tree.get(1), tableReferences);

    expressionCount[0] = expressions.size();

    return new QueryTemplate
      (expressions, source, andExpressions
       (client.server.dbms, makeExpressionFromWhere
        (client.server, tree.get(4), tableReferences), tests));
  }

  private static MyColumn findColumn(MyTable table,
                                   String name)
  {
    MyColumn c = table.columnMap.get(name);
    if (c == null) {
      throw new RuntimeException("no such column: " + name);
    } else {
      return c;
    }
  }

  private static List<Column> makeColumnList(MyTable table,
                                                  Tree tree)
  {
    List<Column> columns = new ArrayList(tree.length());
    for (int i = 0; i < tree.length(); ++i) {
      columns.add(findColumn(table, ((Name) tree.get(i)).value).column);
    }
    return columns;
  }

  private static List<Column> makeOptionalColumnList(MyTable table,
                                                          Tree tree)
  {
    if (tree == Nothing) {
      List<Column> columns = new ArrayList(table.columnList.size());
      for (MyColumn c: table.columnList) {
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
    MyTable table = findTable(client, ((Name) tree.get(2)).value);

    return new InsertTemplate
      (table.table, makeOptionalColumnList(table, tree.get(3)),
       makeExpressionList(client.server, tree.get(6), null),
       DuplicateKeyResolution.Throw);
  }

  private static PatchTemplate makeUpdateTemplate(Client client,
                                                  Tree tree)
  {
    MyTable table = findTable(client, ((Name) tree.get(1)).value);
    MyTableReference tableReference = new MyTableReference
      (table, new TableReference(table.table));
    List<MyTableReference> tableReferences = list(tableReference);

    Tree operations = tree.get(3);
    List<Column> columns = new ArrayList();
    List<Expression> values = new ArrayList();
    for (int i = 0; i < operations.length(); ++i) {
      Tree operation = operations.get(i);
      columns.add(findColumn(table, ((Name) operation.get(0)).value).column);
      values.add
        (makeExpression(client.server, operation.get(2), tableReferences));
    }

    return new UpdateTemplate
      (tableReference.reference, makeExpressionFromWhere
       (client.server, tree.get(4), tableReferences), columns, values);
  }

  private static PatchTemplate makeDeleteTemplate(Client client,
                                                  Tree tree)
  {
    MyTable table = findTable(client, ((Name) tree.get(2)).value);
    MyTableReference tableReference = new MyTableReference
      (table, new TableReference(table.table));
    List<MyTableReference> tableReferences = list(tableReference);

    return new DeleteTemplate
      (tableReference.reference, makeExpressionFromWhere
       (client.server, tree.get(3), tableReferences));
  }

  private static MyColumn findColumn(MyTable table, Column column) {
    for (MyColumn c: table.columnList) {
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
    MyTable table = findTable(client, ((Name) tree.get(1)).value);

    List<Column> columns = makeOptionalColumnList(table, tree.get(2));
    List<Expression> values = new ArrayList(columns.size());
    DBMS dbms = client.server.dbms;
    for (Column c: columns) {
      values.add(new Parameter());
      columnTypes.add(findColumn(table, c).type);
    }

    return new InsertTemplate
      (table.table, columns, values, DuplicateKeyResolution.Throw);
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

  private static MyTable makeTable(Server server,
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
    List<MyColumn> columnList = new ArrayList(columns.size());
    Map<String, MyColumn> columnMap = new HashMap(columns.size());
    for (Tree column: columns) {
      Class type = findColumnType(server, ((Terminal) column.get(1)).value);
      Column dbmsColumn = new Column(type, makeId());
  
      MyColumn myColumn = new MyColumn
        (((Name) column.get(0)).value, dbmsColumn, type);
      columnList.add(myColumn);
      columnMap.put(myColumn.name, myColumn);
    }

    List<MyColumn> myPrimaryKeyColumns
      = new ArrayList(primaryKeyTree.length());

    List<Column> dbmsPrimaryKeyColumns = new ArrayList
      (primaryKeyTree.length());

    for (int i = 0; i < primaryKeyTree.length(); ++i) {
      MyColumn c = columnMap.get(((Name) primaryKeyTree.get(i)).value);
      if (c == null) {
        throw new RuntimeException
          ("primary key refers to non-exisitant column "
           + ((Name) primaryKeyTree.get(i)).value);
      } else {
        myPrimaryKeyColumns.add(c);
        dbmsPrimaryKeyColumns.add(c.column);
      }
    }

    return new MyTable
      (((Name) tree.get(2)).value, columnList, columnMap, myPrimaryKeyColumns,
       new Table(dbmsPrimaryKeyColumns, makeId()));
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
    byte[] array = new byte[readInteger(in)];
    int c;
    int offset = 0;
    while (offset < array.length 
           && (c = in.read(array, offset, array.length - offset)) != -1)
    {
      offset += c;
    }
    return new String(array);
  }

  public static String makeList(Client client,
                                Tree tree)
  {
    DBMS dbms = client.server.dbms;
    StringBuilder sb = new StringBuilder();
    if (tree instanceof Terminal) {
      String type = ((Terminal) tree).value;
      if (type == "databases") {
        QueryResult result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listDatabases);

        while (result.nextRow() == QueryResult.Type.Inserted) {
          sb.append("\n");
          sb.append(((Database) result.nextItem()).name);
        }
      } else if (type == "tables") {
        QueryResult result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTables,
           database(client).name);

        while (result.nextRow() == QueryResult.Type.Inserted) {
          sb.append("\n");
          sb.append(((MyTable) result.nextItem()).name);
        }
      } else if (type == "tags") {
        QueryResult result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTags,
           database(client).name);

        while (result.nextRow() == QueryResult.Type.Inserted) {
          String name = ((Tag) result.nextItem()).name;
          if (! "head".equals(name)) {
            sb.append("\n");
            sb.append(name);
          }
        }
        sb.append("\nhead\ntail");
      } else {
        throw new RuntimeException("unexpected terminal: " + type);
      }
    } else {
      for (MyColumn c: findTable
             (client, ((Name) tree.get(2)).value).columnList)
      {
        sb.append("\n");
        sb.append(c.name);
      }
    }
    return sb.length() == 0 ? "\n no matches found" : sb.toString();
  }

  public static String makeHelp() throws IOException {
    InputStream help = SQLServer.class.getResourceAsStream
      ("/sql-client-help.txt");
    try {
      StringBuilder sb = new StringBuilder();
      char[] buffer = new char[8096];
      Reader in = new InputStreamReader(help);
      int c = 0;
      while ((c = in.read(buffer)) != -1) {
        sb.append(buffer, 0, c);
      }
      return sb.toString();
    } finally {
      help.close();
    }
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
      QueryResult.Type resultType = result.nextRow();
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
    RevisionBuilder builder = dbms.builder(client.transaction.dbHead);
    int count = builder.apply(template, parameters);
    client.transaction.dbHead = builder.commit();
    return count;
  }

  private static void setDatabase(Client client, Database database) {
    apply(client, client.server.insertOrUpdateDatabase, database.name,
          database);
  }

  private static void setTable(Client client, MyTable table) {
    apply(client, client.server.insertOrUpdateTable, database(client).name,
          table.name, table);
  }

  private static void setTag(Client client, Tag tag) {
    if ("tail".equals(tag.name)) {
      throw new RuntimeException("cannot redefine tag \"tail\"");
    }

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
      ConflictResolver conflictResolver = client.server.conflictResolver;
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
    if (client.transaction == null) {
      throw new RuntimeException("no transaction in progress");
    }
    client.transaction = client.transaction.next;
  }

  private static int apply(Client client,
                           PatchTemplate template)
  {
    pushTransaction(client);
    try {
      DBMS dbms = client.server.dbms;
      RevisionBuilder builder = dbms.builder(head(client));
      int count = builder.apply(template);
      setTag(client, new Tag("head", builder.commit()));
      commitTransaction(client);
      return count;
    } finally {
      popTransaction(client);
    }
  }

  private static Object convert(Class type,
                                String value)
  {
    if (type == Integer.class
        || type == Long.class)
    {
      return Long.parseLong(value);
    } else if (type == String.class) {
      return value;
    } else {
      throw new RuntimeException("unexpected type: " + type);
    }
  }

  private static void copy(DBMS dbms,
                           RevisionBuilder builder,
                           PatchTemplate template,
                           List<Class> columnTypes,
                           StringBuilder sb,
                           Object[] parameters,
                           String line)
    throws IOException
  {
    boolean sawEscape = false;
    int index = 0;
    for (int i = 0; i < line.length(); ++i) {
      int c = line.charAt(i);
      switch (c) {
      case '\\':
        if (sawEscape) {
          sawEscape = false;
          sb.append((char) c);
        } else {
          sawEscape = true;
        }
        break;

      case ',':
        if (sawEscape) {
          sawEscape = false;
          sb.append((char) c);
        } else {
          parameters[index] = convert(columnTypes.get(index), sb.toString());
          sb.setLength(0);
          ++ index;
        }
        break;

      default:
        if (sawEscape) {
          sawEscape = false;
          sb.append('\\');
        }
        sb.append((char) c);
        break;
      }
    }

    if (sb.length() == 0 || index < parameters.length - 1) {
      throw new RuntimeException("not enough values specified");
    }

    parameters[index] = convert(columnTypes.get(index), sb.toString());
    sb.setLength(0);
    builder.apply(template, parameters);
  }

  private static void applyCopy(Client client,
                                String line,
                                OutputStream out)
    throws IOException
  {
    CopyContext c = client.copyContext;
    if ("\\.".equals(line)) {
      setTag(client, new Tag("head", c.builder.commit()));
      commitTransaction(client);
      popTransaction(client);
      out.write(Response.Success.ordinal());
      writeString(out, "inserted " + c.count + " row(s)");
      client.copyContext = null;
    } else if (! c.trouble) {
      try {
        copy(client.server.dbms, c.builder, c.template, c.columnTypes,
             c.stringBuilder, c.parameters, line);
        ++ c.count;
      } catch (Exception e) {
        c.trouble = true;
        log.log(Level.WARNING, null, e);
      }
    }
  }

  private static void addCompletion(ParseContext context,
                                    NameType type,
                                    String name)
  {
    if (context.completions == null) {
      context.completions = new HashMap();
    }
    Set<String> set = context.completions.get(type);
    if (set == null) {
      context.completions.put(type, set = new HashSet());
    }
    set.add(name);
  }

  private static Set<String> findCompletions(Client client,
                                             NameType type,
                                             String start)
  {
    DBMS dbms = client.server.dbms;
    switch (type) {
    case Database: {
      QueryResult result = dbms.diff
        (dbms.revision(), dbHead(client), client.server.listDatabases);

      Set<String> set = new HashSet();
      while (result.nextRow() == QueryResult.Type.Inserted) {
        String name = ((Database) result.nextItem()).name;
        if (name.startsWith(start)) {
          set.add(name);
        }
      }
      return set;
    }

    case Table:
      if (client.database != null) {
        QueryResult result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTables,
           client.database.name);

        Set<String> set = new HashSet();
        while (result.nextRow() == QueryResult.Type.Inserted) {
          String name = ((MyTable) result.nextItem()).name;
          if (name.startsWith(start)) {
            set.add(name);
          }
        }
        return set;
      }
      break;

    case Column:
      break;

    case Tag:
      if (client.database != null) {
        QueryResult result = dbms.diff
          (dbms.revision(), dbHead(client), client.server.listTags,
           client.database.name);

        Set<String> set = new HashSet();
        while (result.nextRow() == QueryResult.Type.Inserted) {
          String name = ((Tag) result.nextItem()).name;
          if (name.startsWith(start)) {
            set.add(name);
          }
        }
        return set;
      }
      break;
      
    default: throw new RuntimeException("unexpected name type: " + type);
    }
    
    return null;
  }

  private static Set<String> findCompletions(ParseContext context,
                                             NameType type,
                                             String start)
  {
    if (type == NameType.Column) {
      if (context.completions != null) {
        Set<String> columnSet = context.completions.get(type);
        if (columnSet != null) {
          return columnSet;
        } else {
          Set<String> tableSet = context.completions.get(NameType.Table);
          if (tableSet != null && context.client.database != null) {
            DBMS dbms = context.client.server.dbms;
            Revision tail = dbms.revision();
            Revision head = dbHead(context.client);
            Set<String> columns = new HashSet();
            for (String tableName: tableSet) {
              QueryResult result = dbms.diff
                (tail, head, context.client.server.findTable,
                 context.client.database.name, tableName);

              if (result.nextRow() == QueryResult.Type.Inserted) {
                for (MyColumn c: ((MyTable) result.nextItem()).columnList) {
                  if (c.name.startsWith(start)) {
                    columns.add(c.name);
                  }
                }
              }
            }

            if (columns.size() == 0) {
              return null;
            } else {
              return new HashSet(columns);
            }
          } else {
            return null;
          }
        }
      } else {
        return null;
      }
    } else {
      return findCompletions(context.client, type, start);
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

    public static ParseResult success(Tree tree,
                                      String next,
                                      Set<String> completions)
    {
      return new ParseResult(tree, next, completions);
    }

    public static ParseResult fail(Set<String> completions) {
      return new ParseResult(null, null, completions);
    }

    public static String skipSpace(String in) {
      int i = 0;
      while (i < in.length()) {
        if (Character.isSpace(in.charAt(i))) {
          ++ i;
        } else {
          break;
        }
      }

      return i == 0 ? in : in.substring(i);
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

    public static Parser terminal(final String value,
                                  final boolean completeIfEmpty)
    {
      return new Parser() {
        private final Terminal terminal = new Terminal(value);

        public ParseResult parse(ParseContext context, String in) {
          String token = skipSpace(in);
          if (token.startsWith(value)) {
            return success(terminal, token.substring(value.length()), null);
          } else if ((token.length() > 0 || completeIfEmpty)
                     && (token == context.start || token != in)
                     && value.startsWith(token))
          {
            return fail(set(value));
          } else {
            return fail(null);
          }
        }
      };
    }

    public static Parser terminal(final String value) {
      return terminal(value, true);
    }

    public static Parser or(final Parser ... parsers) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          Set<String> completions = null;
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in);
            if (result.tree != null) {
              return result;
            } else if (result.completions != null) {
              if (completions == null) {
                completions = new HashSet();
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
                return success(list, result.next, result.completions);
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
          ParseResult previous = null;
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in);
            if (result.tree != null) {
              list.add(result.tree);
              if (in.length() > 0) {
                previous = result;
              }
              in = result.next;
            } else {
              if (in.length() == 0 && previous != null) {
                return fail(previous.completions);
              }

              return fail(result.completions);
            }
          }
          return success(list, in, previous.completions);
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
            return success(Nothing, in, result.completions);
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
          String token = skipSpace(in);
          String name = parseName(token);
          if (name != null) {
            if (addCompletion) {
              addCompletion(context, type, name);
            }
            return success
              (new Name(name),
               token.substring(name.length()),
               (token == context.start || token != in) && findCompletions
               ? findCompletions(context, type, name) : null);
          } else {
            return fail
              ((token == context.start || token != in) && findCompletions
               ? findCompletions(context, type, "") : null);
          }
        }
      };
    }

    public static Parser stringLiteral() {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          in = skipSpace(in);
          if (in.length() > 1) {
            StringBuilder sb = new StringBuilder();
            char c = in.charAt(0);
            if (c == '\'') {
              boolean sawEscape = false;
              for (int i = 1; i < in.length(); ++i) {
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
                                   in.substring(i + 1), null);
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
          in = skipSpace(in);
          if (in.length() > 0) {
            char first = in.charAt(0);
            boolean negative = first == '-';
            int start = negative ? 1 : 0;
            int i = start;
            while (i < in.length()) {
              char c = in.charAt(i);
              if (Character.isDigit(c)) {
                ++ i;
              } else {
                break;
              }
            }

            if (i > start) {
              return success(new NumberLiteral(in.substring(0, i)),
                             in.substring(i), null);
            } else {
              return fail(null);
            }
          }
          return fail(null);
        }
      };
    }

    public static Parser booleanLiteral() {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          in = skipSpace(in);
          if (in.startsWith("true")) {
            return success(new BooleanLiteral(true),
                           in.substring(4), null);
          } else if (in.startsWith("false")) {
            return success(new BooleanLiteral(false),
                           in.substring(5), null);
          } else {
            return fail(null);
          }
        }
      };
    }

    public Parser simpleExpression() {
      return or
        (columnName(),
         stringLiteral(),
         numberLiteral(),
         booleanLiteral(),
         sequence(terminal("(", false),
                  expression(),
                  terminal(")")),
         sequence(terminal("not", false),
                  expression()));
    }

    public Parser comparison() {
      return sequence(simpleExpression(),
                      or(terminal("="),
                         terminal("<>"),
                         terminal("<"),
                         terminal("<="),
                         terminal(">"),
                         terminal(">=")),
                      simpleExpression());
    }

    public Parser expression() {
      if (expressionParser == null) {
        expressionParser = new LazyParser();
        expressionParser.parser = or
          (sequence(or(comparison(),
                       simpleExpression()),
                    or(terminal("and"),
                       terminal("or")),
                    expression()),
           comparison(),
           simpleExpression());
      }

      return expressionParser;
    }

    public Parser simpleSource() {
      return or
        (name(NameType.Table, true, true),
         sequence(terminal("(", false),
                  source(),
                  terminal(")")));
    }

    public Parser source() {
      if (sourceParser == null) {
        sourceParser = new LazyParser();
        sourceParser.parser = or
          (sequence(simpleSource(),
                    sequence(or(terminal("left"),
                                terminal("inner")),
                             terminal("join")),
                    source(),
                    terminal("on"),
                    expression()),
           simpleSource());
      }

      return sourceParser;
    }

    public static Parser columnName() {
      return or
        (sequence(name(NameType.Table, false, false),
                  terminal("."),
                  name(NameType.Column, true, false)),
         name(NameType.Column, true, false));
    }

    public Parser select() {
      return task
        (sequence
         (terminal("select"),
          or(terminal("*", false), list(expression())),
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
          list(or(stringLiteral(),
                  numberLiteral(),
                  booleanLiteral())),
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
             client.copyContext = new CopyContext
               (client.server.dbms.builder(head(client)), makeCopyTemplate
                (client, tree, columnTypes), columnTypes);

             pushTransaction(client);

             out.write(Response.CopySuccess.ordinal());
             writeString(out, "reading row data until \"\\.\"");
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
          name(NameType.Tag, false, false),
          name(NameType.Tag, true, false)),
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
                         + conflictResolver.conflictCount + " conflict(s))");
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
             String list = makeList(client, tree.get(1));
             out.write(Response.Success.ordinal());
             writeString(out, list);
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
               RevisionBuilder builder = dbms.builder
                 (client.transaction.dbHead);

               if ("database".equals(type)) {
                 builder.apply(client.server.deleteDatabase, name);
                 builder.apply(client.server.deleteDatabaseTables, name);
                 builder.apply(client.server.deleteDatabaseTags, name);
               } else if ("table".equals(type)) {
                 builder.apply(client.server.deleteTable,
                            database(client).name, name);
               } else if ("tag".equals(type)) {
                 builder.apply(client.server.deleteTag,
                            database(client).name, name);
               } else {
                 throw new RuntimeException();
               }

               client.transaction.dbHead = builder.commit();

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

             out.write(Response.NewDatabase.ordinal());
             writeString(out, name);
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
             String help = makeHelp();
             out.write(Response.Success.ordinal());
             writeString(out, help);
           }
         });
    }
  }

  private static void executeRequest(Client client,
                                     InputStream in,
                                     OutputStream out)
    throws IOException
  {
    String s = readString(in);
    try {
      if (client.copyContext == null) {
        log.info("execute \"" + s + "\"");
        ParseResult result = client.server.parser.parse
          (new ParseContext(client, s), s);
        if (result.task != null) {
          result.task.run(client, result.tree, in, out);
        } else {
          out.write(Response.Error.ordinal());
          writeString(out, "Sorry, I don't understand.");
        }
      } else {
        applyCopy(client, s, out);
      }
    } catch (Exception e) {
      out.write(Response.Error.ordinal());
      String message = e.getMessage();
      writeString(out, message == null ? "see server logs" : message); 
      log.log(Level.WARNING, null, e);       
    }
  }

  private static void completeRequest(Client client,
                                      InputStream in,
                                      OutputStream out)
    throws IOException
  {
    String s = readString(in);
    log.info("complete \"" + s + "\"");
    if (client.copyContext == null) {
      ParseResult result = client.server.parser.parse
        (new ParseContext(client, s), s);
      out.write(Response.Success.ordinal());
      if (result.completions == null) {
        log.info("no completions");
        writeInteger(out, 0);
      } else {
        log.info("completions: " + result.completions);
        writeInteger(out, result.completions.size());
        for (String completion: result.completions) {
          writeString(out, completion);
        }
      }
    } else {
      log.info("no completions in copy mode");
      out.write(Response.Success.ordinal());
      writeInteger(out, 0);
    }
  }

  private static void handleRequest(Client client,
                                    InputStream in,
                                    OutputStream out)
    throws IOException
  {
    int requestType = in.read();
    if (requestType == -1) {
      client.channel.close();
      return;
    }

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
