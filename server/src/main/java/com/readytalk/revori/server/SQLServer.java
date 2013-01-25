/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.server;

import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.util.Util.convert;
import static com.readytalk.revori.util.Util.set;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.BinaryOperation.Type;
import com.readytalk.revori.Column;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.Comparators;
import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.Constant;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.DuplicateKeyResolution;
import com.readytalk.revori.Expression;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.Join;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.QueryResult;
import com.readytalk.revori.QueryTemplate;
import com.readytalk.revori.QueryTemplate.OrderExpression;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Source;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;
import com.readytalk.revori.UnaryOperation;
import com.readytalk.revori.UpdateTemplate;
import com.readytalk.revori.server.protocol.Stringable;
import com.readytalk.revori.server.simple.SimpleRevisionServer;
import com.readytalk.revori.subscribe.Subscription;
import com.readytalk.revori.util.BufferOutputStream;

@NotThreadSafe
public class SQLServer implements RevisionServer {
  private static final boolean Debug = true;

  private static final Logger log = LoggerFactory.getLogger(SQLServer.class);

  private enum Request {
    Execute, Complete;
  }

  public enum Response {
    RowSet, NewDatabase, CopySuccess, Success, Error;
  }

  public enum RowSetFlag {
    InsertedRow, DeletedRow, End, Item;
  }

  private static final int ThreadPoolSize = 256;

  private static final Tree Nothing = new Nothing();

  private static final Map<Class, Validator> validators = Maps.newHashMap();

  static {
    validators.put(Parameter.class, new Validator<Parameter>() {
      public Expression validate(Class type, Parameter expression) {
        // todo
        throw new UnsupportedOperationException();
      }
    });

    validators.put(Constant.class, new Validator<Constant>() {
      public Expression validate(Class type, Constant expression) {
        if(expression.value instanceof Literal) {
          return new Constant(parse(type, ((Literal)expression.value).value, false)); 
        } else if(expression.value instanceof StringLiteral) {
          return new Constant(parse(type, ((StringLiteral)expression.value).value, true)); 
        } else if (type == null
            || expression.value == null
            || type.isInstance(expression.value))
        {
          return expression;
        } else {
          throw new RuntimeException("couldn't validate value: " + expression.value.toString());
        }
      }
    });

    validators.put(ColumnReference.class, new Validator<Expression>() {
      public Expression validate(Class type, Expression expression) {
        return expression;
      }
    });

    validators.put(UnaryOperation.class, new Validator<UnaryOperation>() {
      public Expression validate(Class<UnaryOperation> type, UnaryOperation expression) {
        switch (expression.type.operationClass()) {
        case Boolean: {
          Expression operand = SQLServer.validate
            (Boolean.class, expression.operand);
          if (operand == expression.operand) {
            return expression;
          } else {
            return new UnaryOperation(expression.type, operand);
          }
        }
          
        default: throw new RuntimeException
            ("unexpected operation class: "
             + expression.type.operationClass());
        }
      }
    });

    validators.put(BinaryOperation.class, new Validator<BinaryOperation>() {
      public Expression validate(Class<BinaryOperation> type, BinaryOperation expression) {
        Expression left = SQLServer.validate(null, expression.leftOperand);
        Expression right = SQLServer.validate
          (left.typeConstraint(), expression.rightOperand);

        left = SQLServer.validate
          (right.typeConstraint(), expression.leftOperand);

        if (left == expression.leftOperand
            && right == expression.rightOperand)
        {
          return expression;
        } else {
          return new BinaryOperation(expression.type, left, right);
        }
      }
    });
  }

  private interface Validator<T extends Expression> {
    public Expression validate(@Nullable Class<T> type, T expression);
  }
  
  @NotThreadSafe
  private static class Server {
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

    public final Column<String> tagsDatabase;
    public final Column<String> tagsName;
    public final Column<Object> tagsTag;
    public final Table tags;

    public final PatchTemplate insertOrUpdateTag;
    public final QueryTemplate listTags;
    public final QueryTemplate findTag;
    public final PatchTemplate deleteDatabaseTags;
    public final PatchTemplate deleteTag;

    public final Map<String, BinaryOperation.Type> binaryOperationTypes
      = new HashMap<String, Type>();
    public final Map<String, UnaryOperation.Type> unaryOperationTypes
      = new HashMap<String, com.readytalk.revori.UnaryOperation.Type>();
    public final Map<String, Class> columnTypes = new HashMap<String, Class>();
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
              (((Tag) leftValue).name,
                  ((baseValue == null ? Revisions.Empty : ((Tag) baseValue).revision)).merge(
                ((Tag) leftValue).revision,
                ((Tag) rightValue).revision,
                rightPreferenceConflictResolver,
                ForeignKeyResolvers.Delete));
          } else {
            return rightValue;
          }
        }
      };
    public final RevisionServer server = new SimpleRevisionServer
      (conflictResolver, ForeignKeyResolvers.Delete);

    public Server() {
      Column<String> databasesName = new Column<String>(String.class);
      Column<Object> databasesDatabase = new Column<Object>(Object.class);
      Table databases = new Table(cols(databasesName));
      TableReference databasesReference = new TableReference(databases);

      this.insertOrUpdateDatabase = new InsertTemplate
        (databases, cols(databasesName, databasesDatabase),
         Lists.newArrayList((Expression) new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listDatabases = new QueryTemplate
        (Lists.newArrayList(reference(databasesReference, databasesDatabase)),
         databasesReference,
         new Constant(true));

      this.findDatabase = new QueryTemplate
        (Lists.newArrayList(reference(databasesReference, databasesDatabase)),
         databasesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(databasesReference, databasesName),
          new Parameter()));

      this.deleteDatabase = new DeleteTemplate
        (databasesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(databasesReference, databasesName),
          new Parameter()));

      Column<String> tablesDatabase = new Column<String>(String.class);
      Column<String> tablesName = new Column<String>(String.class);
      Column<Object> tablesTable = new Column<Object>(Object.class);
      Table tables = new Table(cols(tablesDatabase, tablesName));
      TableReference tablesReference = new TableReference(tables);

      this.insertOrUpdateTable = new InsertTemplate
        (tables, cols(tablesDatabase, tablesName, tablesTable),
         Lists.newArrayList((Expression) new Parameter(), new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listTables = new QueryTemplate
        (Lists.newArrayList(reference(tablesReference, tablesTable)),
         tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(tablesReference, tablesDatabase),
          new Parameter()));

      this.findTable = new QueryTemplate
        (Lists.newArrayList(reference(tablesReference, tablesTable)),
         tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tablesReference, tablesDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tablesReference, tablesName),
           new Parameter())));

      this.deleteDatabaseTables = new DeleteTemplate
        (tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(tablesReference, tablesDatabase),
          new Parameter()));

      this.deleteTable = new DeleteTemplate
        (tablesReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tablesReference, tablesDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tablesReference, tablesName),
           new Parameter())));

      this.tagsDatabase = new Column<String>(String.class);
      this.tagsName = new Column<String>(String.class);
      this.tagsTag = new Column<Object>(Object.class);
      this.tags = new Table(cols(tagsDatabase, tagsName));
      TableReference tagsReference = new TableReference(tags);

      this.insertOrUpdateTag = new InsertTemplate
        (tags, cols(tagsDatabase, tagsName, tagsTag),
         Lists.newArrayList((Expression) new Parameter(), new Parameter(), new Parameter()),
         DuplicateKeyResolution.Overwrite);

      this.listTags = new QueryTemplate
        (Lists.newArrayList(reference(tagsReference, tagsTag)),
         tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(tagsReference, tagsDatabase),
          new Parameter()));

      this.findTag = new QueryTemplate
        (Lists.newArrayList(reference(tagsReference, tagsTag)),
         tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tagsReference, tagsDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tagsReference, tagsName),
           new Parameter())));

      this.deleteDatabaseTags = new DeleteTemplate
        (tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.Equal,
          reference(tagsReference, tagsDatabase),
          new Parameter()));

      this.deleteTag = new DeleteTemplate
        (tagsReference,
         new BinaryOperation
         (BinaryOperation.Type.And,
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tagsReference, tagsDatabase),
           new Parameter()),
          new BinaryOperation
          (BinaryOperation.Type.Equal,
           reference(tagsReference, tagsName),
           new Parameter())));

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

    public Transaction(@Nullable Transaction next,
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
    @Nullable
    public final SocketChannel channel;
    @Nullable
    public Transaction transaction;
    public Database database;
    public CopyContext copyContext;

    public Client(Server server,
                  @Nullable SocketChannel channel)
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
        log.error("Problem with channel.", e);
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
    public final Column<?> column;
    public final Class<?> type;
    
    public MyColumn(String name,
                  Column<?> column,
                  Class<?> type)
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
    public final boolean lastAtomic;
    public Task task;

    public ParseResult(@Nullable Tree tree,
                       @Nullable String next,
                       @Nullable Set<String> completions,
                       boolean lastAtomic)
    {
      this.tree = tree;
      this.next = next;
      this.completions = completions;
      this.lastAtomic = lastAtomic;
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
    public ParseResult parse(ParseContext context, String in, boolean lastAtomic);
  }

  private static class LazyParser implements Parser {
    public Parser parser;

    public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
      if (context.depth++ > 10) throw new RuntimeException("debug");

      return parser.parse(context, in, lastAtomic);
    }
  }

  private interface Tree {
    public Tree get(int index);
    public int length();
  }

  private static class TreeList implements Tree {
    public final List<Tree> list = new ArrayList<Tree>();
    
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

  private static abstract class Leaf implements Tree {
    public Tree get(int index) {
      throw new UnsupportedOperationException();
    }

    public int length() {
      throw new UnsupportedOperationException();
    }
  }

  private static class Nothing extends Leaf {
    public String toString() {
      return "nothing";
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

  private static class Literal extends Leaf {
    public final String value;

    public Literal(String value) {
      this.value = value;
    }

    public String toString() {
      return "literal[" + value + "]";
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
      return client.server.server.head();
    }
  }

  private static Database findDatabase(Client client,
                                       String name)
  {
    Server server = client.server;
    QueryResult result = Revisions.Empty.diff
      (dbHead(client), server.findDatabase, name);

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

    if ("tail".equals(name)) {
      return new Tag(name, Revisions.Empty);
    }

    QueryResult result = Revisions.Empty.diff
      (dbHead(client), server.findTag, database(client).name,
       name);

    if (result.nextRow() == QueryResult.Type.Inserted) {
      return (Tag) result.nextItem();
    } else if ("head".equals(name)) {
      return new Tag(name, Revisions.Empty);
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
    QueryResult result = Revisions.Empty.diff
      (dbHead(client), client.server.findTable,
       database(client).name, name);

    if (result.nextRow() == QueryResult.Type.Inserted) {
      return (MyTable) result.nextItem();
    } else {
      throw new RuntimeException("no such table: " + name);
    }
  }

  private static Expression validate(@Nullable Class type,
                                     Expression expression)
  {
    return validators.get(expression.getClass()).validate(type, expression);
  }

  private static Object parse(Class type, String value, boolean quoted) {
    if("null".equals(value) && (type != String.class || !quoted)) {
      return null;
    } else if (Stringable.class.isAssignableFrom(type)) {
      try {
        return type.getConstructor(String.class).newInstance(value);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (java.lang.reflect.InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } else if (type == Long.class) {
      return Long.parseLong(value);
    } else if (type == Integer.class) {
      return Integer.parseInt(value);
    } else if (type == Short.class) {
      return Short.parseShort(value);
    } else if (type == Byte.class) {
      return Byte.parseByte(value);
    } else if (type == Boolean.class) {
      return Boolean.parseBoolean(value);
    } else if(type == String.class) {
      return value;
    }
    // todo: handle types like byte arrays, enums, etc., and use a map
    // of types to parsers instead of a big if/else chain

    throw new RuntimeException
      ("don't know how to parse \"" + value + "\" as a " + type.getName());
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
    (List<MyTableReference> tableReferences,
     String tableName,
     String columnName)
  {
    TableReference reference = null;
    Column<?> column = null;
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

  private static OrderExpression makeOrderExpression
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    Comparator comparator;
    if (tree.length() == 2) {
      Tree t = tree.get(1);
      if(t == Nothing) {
        comparator = Comparators.Ascending;
      } else {
        String sort = ((Terminal) t).value;
        if ("desc".equals(sort)) {
          comparator = Comparators.Descending;
        } else if ("asc".equals(sort)) {
          comparator = Comparators.Ascending;
        } else {
          throw new RuntimeException("unrecognized sort: \"" + sort + "\"");
        }
      }
    } else {
      comparator = Comparators.Ascending;
    }
    return new OrderExpression
      (makeExpression(server, tree.get(0), tableReferences), comparator);
  }

  private static Expression makeExpression
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    if (tree instanceof Name) {
      return makeColumnReference
        (tableReferences, null, ((Name) tree).value);
    } else if (tree instanceof Literal || tree instanceof StringLiteral) {
      // the validator will later take the literal and unwrap it, because it knows the expected type
      return new Constant(tree);
    } if (tree.length() == 3) {
      if (tree.get(0) instanceof Name
          && ".".equals(((Terminal) tree.get(1)).value))
      {
        return makeColumnReference
          (tableReferences,
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
    List<Expression> expressions = new ArrayList<Expression>();
    if (tree instanceof Terminal) {
      for (MyTableReference tableReference: tableReferences) {
        for (MyColumn column: tableReference.table.columnList) {
          expressions.add
            (reference(tableReference.reference, column.column));
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

  private static List<OrderExpression> makeOrderExpressionList
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    List<OrderExpression> expressions = new ArrayList<OrderExpression>();
    for (int i = 0; i < tree.length(); ++i) {
      expressions.add(makeOrderExpression(server, tree.get(i), tableReferences));
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

  private static Set<Expression> makeExpressionsFromGroupBy
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    if (tree == Nothing) {
      return Collections.<Expression>emptySet();
    } else {
      return new HashSet<Expression>(makeExpressionList(server, tree.get(2), tableReferences));
    }
  }

  private static List<OrderExpression> makeOrderExpressionsFromOrderBy
    (Server server,
     Tree tree,
     List<MyTableReference> tableReferences)
  {
    if (tree == Nothing) {
      return Collections.<OrderExpression>emptyList();
    } else {
      return makeOrderExpressionList(server, tree.get(2), tableReferences);
    }
  }

  private static Expression andExpressions(Expression expression,
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
    List<MyTableReference> tableReferences = new ArrayList<MyTableReference>();
    List<Expression> tests = new ArrayList<Expression>();
    Source source = makeSource
      (client, tree.get(3), tableReferences, tests);

    List<Expression> expressions = makeExpressionList
      (client.server, tree.get(1), tableReferences);

    expressionCount[0] = expressions.size();
    
    // System.out.println(tree.toString());
    
    return new QueryTemplate(expressions,
        source,
        validate(Boolean.class,
         andExpressions(
           makeExpressionFromWhere(client.server, tree.get(4), tableReferences),
           tests)),
         makeExpressionsFromGroupBy(client.server, tree.get(5), tableReferences),
         makeOrderExpressionsFromOrderBy(client.server, tree.get(6), tableReferences));
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

  private static List<Column<?>> makeColumnList(MyTable table,
                                                  Tree tree)
  {
    List<Column<?>> columns = new ArrayList<Column<?>>(tree.length());
    for (int i = 0; i < tree.length(); ++i) {
      columns.add(findColumn(table, ((Name) tree.get(i)).value).column);
    }
    return columns;
  }

  private static List<Column<?>> makeOptionalColumnList(MyTable table,
                                                          Tree tree)
  {
    if (tree == Nothing) {
      List<Column<?>> columns = new ArrayList<Column<?>>(table.columnList.size());
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

    List<Column<?>> columns = makeOptionalColumnList(table, tree.get(3));
    List<Expression> rawValues = makeExpressionList
      (client.server, tree.get(6), null);

    if (columns.size() != rawValues.size()) {
      throw new RuntimeException("column count and value count do not match");
    }

    List<Expression> values = new ArrayList<Expression>(rawValues.size());
    for (int i = 0; i < columns.size(); ++i) {
      values.add(validate(columns.get(i).type, rawValues.get(i)));
    }

    return new InsertTemplate
      (table.table, columns, values, DuplicateKeyResolution.Throw);
  }

  private static PatchTemplate makeUpdateTemplate(Client client,
                                                  Tree tree)
  {
    MyTable table = findTable(client, ((Name) tree.get(1)).value);
    MyTableReference tableReference = new MyTableReference
      (table, new TableReference(table.table));
    List<MyTableReference> tableReferences = Lists.newArrayList(tableReference);

    Tree operations = tree.get(3);
    List<Column<?>> columns = new ArrayList<Column<?>>();
    List<Expression> values = new ArrayList<Expression>();
    for (int i = 0; i < operations.length(); ++i) {
      Tree operation = operations.get(i);
      Column<?> column = findColumn
        (table, ((Name) operation.get(0)).value).column;
      columns.add(column);
      values.add
        (validate
         (column.type, makeExpression
          (client.server, operation.get(2), tableReferences)));
    }

    return new UpdateTemplate
      (tableReference.reference, validate
       (Boolean.class, makeExpressionFromWhere
        (client.server, tree.get(4), tableReferences)), columns, values);
  }

  private static PatchTemplate makeDeleteTemplate(Client client,
                                                  Tree tree)
  {
    MyTable table = findTable(client, ((Name) tree.get(2)).value);
    MyTableReference tableReference = new MyTableReference
      (table, new TableReference(table.table));
    List<MyTableReference> tableReferences = Lists.newArrayList(tableReference);

    return new DeleteTemplate
      (tableReference.reference, validate
       (Boolean.class, makeExpressionFromWhere
        (client.server, tree.get(3), tableReferences)));
  }

  private static MyColumn findColumn(MyTable table, Column<?> column) {
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

    List<Column<?>> columns = makeOptionalColumnList(table, tree.get(2));
    List<Expression> values = new ArrayList<Expression>(columns.size());
    for (Column<?> c: columns) {
      values.add(new Parameter());
      columnTypes.add(findColumn(table, c).type);
    }

    return new InsertTemplate
      (table.table, columns, values, DuplicateKeyResolution.Throw);
  }

  private static Class<?> findColumnType(Server server,
                                      String name)
  {
    Class<?> type = server.columnTypes.get(name);
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
    List<Tree> columns = new ArrayList<Tree>();
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

    List<MyColumn> columnList = new ArrayList<MyColumn>(columns.size());
    Map<String, MyColumn> columnMap = new HashMap<String, MyColumn>(columns.size());
    for (Tree column: columns) {
      Class<?> type = findColumnType(server, ((Terminal) column.get(1)).value);
      Column<?> dbmsColumn = new Column(type);
  
      MyColumn myColumn = new MyColumn
        (((Name) column.get(0)).value, dbmsColumn, type);
      columnList.add(myColumn);
      columnMap.put(myColumn.name, myColumn);
    }

    List<MyColumn> myPrimaryKeyColumns
      = new ArrayList<MyColumn>(primaryKeyTree.length());

    List<Column<?>> dbmsPrimaryKeyColumns = new ArrayList<Column<?>>
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
       new Table(dbmsPrimaryKeyColumns));
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

  public static String tokenize(String in) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < in.length(); ++i) {
      char c = in.charAt(i);
      boolean isParen = c == '(' || c == ')';
      if (isParen && i > 0 && (! Character.isWhitespace(in.charAt(i - 1)))) {
        sb.append(' ');
      }
      sb.append(c);
      if (isParen && i + 1 < in.length()
          && (! Character.isWhitespace(in.charAt(i + 1))))
      {
        sb.append(' ');
      }
    }
    return sb.toString();
  }

  public static String makeList(Client client,
                                Tree tree)
  {
    StringBuilder sb = new StringBuilder();
    if (tree instanceof Terminal) {
      String type = ((Terminal) tree).value;
      if (type == "databases") {
        QueryResult result = Revisions.Empty.diff
          (dbHead(client), client.server.listDatabases);

        while (result.nextRow() == QueryResult.Type.Inserted) {
          sb.append("\n");
          sb.append(((Database) result.nextItem()).name);
        }
      } else if (type == "tables") {
        QueryResult result = Revisions.Empty.diff
          (dbHead(client), client.server.listTables,
           database(client).name);

        while (result.nextRow() == QueryResult.Type.Inserted) {
          sb.append("\n");
          sb.append(((MyTable) result.nextItem()).name);
        }
      } else if (type == "tags") {
        QueryResult result = Revisions.Empty.diff
          (dbHead(client), client.server.listTags,
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

  private static void diff(Revision base,
                           Revision fork,
                           QueryTemplate template,
                           int expressionCount,
                           OutputStream out)
    throws IOException
  {
    QueryResult result = base.diff(fork, template);

    boolean wroteSentinal = false;

    while (true) {
      QueryResult.Type resultType = result.nextRow();

      if (! wroteSentinal) {
        out.write(Response.RowSet.ordinal());
        wroteSentinal = true;
      }

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
    RevisionBuilder builder = client.transaction.dbHead.builder();
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
      (next, next == null ? client.server.server.head() : next.dbHead);
  }

  private static void commitTransaction(Client client) {
    if (client.transaction.next == null) {
      client.server.server.merge
        (client.transaction.dbTail, client.transaction.dbHead);
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
      RevisionBuilder builder = head(client).builder();
      int count = builder.apply(template);
      setTag(client, new Tag("head", builder.commit()));
      commitTransaction(client);
      return count;
    } finally {
      popTransaction(client);
    }
  }

  private static void copy(RevisionBuilder builder,
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
        copy(c.builder, c.template, c.columnTypes,
             c.stringBuilder, c.parameters, line);
        ++ c.count;
      } catch (Exception e) {
        c.trouble = true;
        log.warn("Trouble with copy operation.", e);
      }
    }
  }

  private static void addCompletion(ParseContext context,
                                    NameType type,
                                    String name)
  {
    if (context.completions == null) {
      context.completions = new HashMap<NameType, Set<String>>();
    }
    Set<String> set = context.completions.get(type);
    if (set == null) {
      context.completions.put(type, set = new HashSet<String>());
    }
    set.add(name);
  }

  private static Set<String> findCompletions(Client client,
                                             NameType type,
                                             String start)
  {
    switch (type) {
    case Database: {
      QueryResult result = Revisions.Empty.diff
        (dbHead(client), client.server.listDatabases);

      Set<String> set = new HashSet<String>();
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
        QueryResult result = Revisions.Empty.diff
          (dbHead(client), client.server.listTables,
           client.database.name);

        Set<String> set = new HashSet<String>();
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
        QueryResult result = Revisions.Empty.diff
          (dbHead(client), client.server.listTags,
           client.database.name);

        Set<String> set = new HashSet<String>();
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
            Revision tail = Revisions.Empty;
            Revision head = dbHead(context.client);
            Set<String> columns = new HashSet<String>();
            for (String tableName: tableSet) {
              QueryResult result = tail.diff
                (head, context.client.server.findTable,
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
              return new HashSet<String>(columns);
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
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          ParseResult result = parser.parse(context, in, lastAtomic);
          if (result.tree != null) {
            result.task = task;
          }
          return result;
        }
      };
    }

    public static ParseResult success(Tree tree,
                                      String next,
                                      @Nullable Set<String> completions,
                                      boolean lastAtomic)
    {
      return new ParseResult(tree, next, completions, lastAtomic);
    }

    public static ParseResult fail(@Nullable Set<String> completions) {
      // TODO: is passing false for lastAtomic here correct?
      return new ParseResult(null, null, completions, false);
    }

    public static String skipSpace(String in) {
      int i = 0;
      while (i < in.length()) {
        if (Character.isWhitespace(in.charAt(i))) {
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
                                  final boolean completeIfEmpty,
                                  final boolean atomic)
    {
      return new Parser() {
        private final Terminal terminal = new Terminal(value);

        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          String token = skipSpace(in);
          if (token.startsWith(value) && (atomic || lastAtomic || token.length() < in.length())) {
            return success(terminal, token.substring(value.length()), null, lastAtomic);
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

    public static Parser symbol(final String value) {
      return symbol(value, true);
    }

    public static Parser symbol(final String value, boolean completeIfEmpty) {
      return terminal(value, completeIfEmpty, true);
    }

    public static Parser terminal(final String value) {
      return terminal(value, true);
    }

    public static Parser terminal(final String value, boolean completeIfEmpty) {
      return terminal(value, completeIfEmpty, false);
    }

    public static Parser or(final Parser ... parsers) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          Set<String> completions = null;
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in, lastAtomic);
            if (result.tree != null) {
              return result;
            } else if (result.completions != null) {
              if (completions == null) {
                completions = new HashSet<String>();
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
        private final Parser comma = symbol(",");

        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          TreeList list = new TreeList();
          while (true) {
            ParseResult result = parser.parse(context, in, lastAtomic);
            if (result.tree != null) {
              list.add(result.tree);

              ParseResult commaResult = comma.parse(context, result.next, result.lastAtomic);
              if (commaResult.tree != null) {
                in = commaResult.next;
                lastAtomic = commaResult.lastAtomic;
              } else {
                return success(list, result.next, result.completions, result.lastAtomic);
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
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          TreeList list = new TreeList();
          ParseResult previous = null;
          for (Parser parser: parsers) {
            ParseResult result = parser.parse(context, in, lastAtomic);
            if (result.tree != null) {
              list.add(result.tree);
              if (in.length() > 0) {
                previous = result;
              }
              in = result.next;
              lastAtomic = result.lastAtomic;
            } else {
              if (in.length() == 0 && previous != null) {
                return fail(previous.completions);
              }

              return fail(result.completions);
            }
          }
          return success(list, in, previous.completions, previous.lastAtomic);
        }
      };
    }

    public static Parser optional(final Parser parser) {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          ParseResult result = parser.parse(context, in, lastAtomic);
          if (result.tree != null) {
            return result;
          } else {
            return success(Nothing, in, result.completions, lastAtomic);
          }
        }
      };
    }

    public static Parser name(final NameType type,
                              final boolean findCompletions,
                              final boolean addCompletion)
    {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          String token = skipSpace(in);
          String name = parseName(token);
          if (name != null && (lastAtomic || token.length() < in.length())) {
            if (addCompletion) {
              addCompletion(context, type, name);
            }
            return success
              (new Name(name),
               token.substring(name.length()),
               (token == context.start || token != in) && findCompletions
               ? findCompletions(context, type, name) : null,
               false);
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
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
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
                                   in.substring(i + 1), null, true);
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
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          String begin = in;
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

            if (i > start && (lastAtomic || in.length() < begin.length())) {
              return success(new Literal(in.substring(0, i)),
                             in.substring(i), null, false);
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
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          String begin = in;
          in = skipSpace(in);
          if(lastAtomic || in.length() < begin.length()) {
            if (in.startsWith("true")) {
              return success(new Literal("true"),
                             in.substring(4), null, false);
            } else if (in.startsWith("false")) {
              return success(new Literal("false"),
                             in.substring(5), null, false);
            } else {
              return fail(null);
            }
          } else {
            return fail(null);
          }
        }
      };
    }


    public static Parser nullLiteral() {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in, boolean lastAtomic) {
          String begin = in;
          in = skipSpace(in);
          if(lastAtomic || in.length() < begin.length()) {
            if (in.startsWith("null")) {
              return success(new Literal("null"),
                             in.substring(4), null, false);
            } else {
              return fail(null);
            }
          } else {
            return fail(null);
          }
        }
      };
    }
    public Parser simpleExpression() {
      return or
        (booleanLiteral(),
         nullLiteral(),
         columnName(),
         stringLiteral(),
         numberLiteral(),
         sequence(symbol("(", false),
                  expression(),
                  symbol(")")),
         sequence(terminal("not", false),
                  expression()));
    }

    public Parser comparison() {
      return sequence(simpleExpression(),
                      or(symbol("="),
                          symbol("<>"),
                          symbol("<"),
                          symbol("<="),
                          symbol(">"),
                          symbol(">=")),
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
         sequence(symbol("(", false),
                  source(),
                  symbol(")")));
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
            symbol("."),
                  name(NameType.Column, true, false)),
         name(NameType.Column, true, false));
    }

    public Parser select() {
      return task
        (sequence
         (terminal("select"),
          or(symbol("*", false), list(expression())),
          terminal("from"),
          source(),
          optional(sequence(terminal("where"),
                            expression())),
          optional(sequence(terminal("group"), terminal("by"),
              list(expression()))),
          optional(sequence(terminal("order"), terminal("by"),
              list(sequence(expression(), optional(or(terminal("desc"), terminal("asc")))))))),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             int[] expressionCount = new int[1];
             SQLServer.diff
               (Revisions.Empty, head(client), makeQueryTemplate
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
             int[] expressionCount = new int[1];
             SQLServer.diff
               (findTag(client, ((Name) tree.get(1)).value).revision,
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
          optional(sequence(symbol("("),
                            list(columnName()),
                            symbol(")"))),
          terminal("values"),
          symbol("("),
          list(or(stringLiteral(),
                  numberLiteral(),
                  booleanLiteral(),
                  nullLiteral())),
          symbol(")")),
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
                        symbol("="),
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
          optional(sequence(symbol("("),
                            list(columnName()),
                            symbol(")"))),
          terminal("from"),
          terminal("stdin")),
         new Task() {
           public void run(Client client,
                           Tree tree,
                           InputStream in,
                           OutputStream out)
             throws IOException
           {
             List<Class> columnTypes = new ArrayList<Class>();
             client.copyContext = new CopyContext
               (head(client).builder(), makeCopyTemplate
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
                       findTag(client, ((Name) tree.get(1)).value).revision.merge
                       (findTag(client, ((Name) tree.get(2)).value).revision,
                        findTag(client, ((Name) tree.get(3)).value).revision,
                        conflictResolver,
                        ForeignKeyResolvers.Delete)));
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
               RevisionBuilder builder = client.transaction.dbHead.builder();

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
          symbol("("),
          list(or(sequence(terminal("primary"),
                           terminal("key"),
                           terminal("("),
                           list(name(NameType.Column, true, false)),
                           symbol(")")),
                  sequence(name(NameType.Column, false, true),
                           or(terminal("int32"),
                              terminal("int64"),
                              terminal("string"),
                              terminal("array"))))),
          symbol(")")),
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
    String s = tokenize(readString(in));
    try {
      if (client.copyContext == null) {
        log.debug("execute \"{}\"", s);
        ParseResult result = client.server.parser.parse
          (new ParseContext(client, s), s, true);
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
      writeString(out, message == null ? e.getClass().getName() : message); 
      log.warn("Problem executing request.", e);
    }
  }

  private static void completeRequest(Client client,
                                      InputStream in,
                                      OutputStream out)
    throws IOException
  {
    String s = tokenize(readString(in));
    log.debug("complete \"{}\"", s);
    if (client.copyContext == null) {
      ParseResult result = client.server.parser.parse
        (new ParseContext(client, s), s, true);
      out.write(Response.Success.ordinal());
      if (result.completions == null) {
        log.debug("no completions");
        writeInteger(out, 0);
      } else {
        log.debug("completions: {}", result.completions);
        writeInteger(out, result.completions.size());
        for (String completion: result.completions) {
          writeString(out, completion);
        }
      }
    } else {
      log.debug("no completions in copy mode");
      out.write(Response.Success.ordinal());
      writeInteger(out, 0);
    }
  }

  private static boolean handleRequest(Client client,
                                       InputStream in,
                                       OutputStream out)
    throws IOException
  {
    int requestType = in.read();
    if (requestType == -1) {
      if (client.channel != null) {
        client.channel.close();
      }
      return false;
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

    return true;
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

    Server server = new Server();

    while (true) {
      executor.execute(new Client(server, serverChannel.accept()));
    }
  }

  private final Server server = new Server();
  private final String database;

  public SQLServer(String database) {
    this.database = database;
  }

  public Connection makeConnection() {
    return new Connection() {
      private final Client client = new Client(server, null);

      public InputStream execute(String command) {
        BufferOutputStream buffer = new BufferOutputStream();
        BufferOutputStream out = new BufferOutputStream();

        try {
          writeString(buffer, command);

          executeRequest
            (client, new ByteArrayInputStream
             (buffer.getBuffer(), 0, buffer.size()), out);
        } catch (IOException e) {
          // should not be possible, since we're reading from and
          // writing to memory
          throw new RuntimeException(e);
        }

        return new ByteArrayInputStream(out.getBuffer(), 0, out.size());
      }      
    };
  }

  private Revision head(Revision dbHead) {
    QueryResult result = Revisions.Empty.diff
      (dbHead, server.findTag, database, "head");

    if (result.nextRow() == QueryResult.Type.Inserted) {
      return ((Tag) result.nextItem()).revision;
    } else {
      return Revisions.Empty;
    }
  }

  public Revision head() {
    return head(server.server.head());
  }

  public void merge(Revision base, Revision fork) {
    Revision dbHead = server.server.head();
    RevisionBuilder builder = dbHead.builder();
    builder.apply
      (server.insertOrUpdateTag, database, "head", new Tag
       ("head", head(dbHead).merge
        (base, fork, server.rightPreferenceConflictResolver,
         ForeignKeyResolvers.Delete)));

    server.server.merge(dbHead, builder.commit());
  }

  public Subscription registerListener(Runnable listener) {
    return server.server.registerListener(listener);
  }

  public void add(Table table, List<Column<?>> columns) {
    Map<String, MyColumn> map = new HashMap<String, MyColumn>(columns.size());
    List<MyColumn> myColumns = new ArrayList<MyColumn>(columns.size());
    for (Column<?> c: columns) {
      MyColumn myColumn = new MyColumn(c.id, c, c.type);
      map.put(c.id, myColumn);
      myColumns.add(myColumn);
    }

    List<Column<?>> primaryKey = table.primaryKey.columns;
    List<MyColumn> myPrimaryKey = new ArrayList<MyColumn>(primaryKey.size());
    for (Column<?> c: primaryKey) {
      myPrimaryKey.add(new MyColumn(c.id, c, c.type));
    }

    Revision base = server.server.head();
    RevisionBuilder builder = base.builder();

    builder.apply
      (server.insertOrUpdateDatabase, database, new Database(database));

    builder.apply
      (server.insertOrUpdateTable, database, table.id,
       new MyTable(table.id, myColumns, map, myPrimaryKey, table));

    server.server.merge(base, builder.commit());
  }

  public void accept(InputStream in,
                     OutputStream out)
    throws IOException
  {
    Client client = new Client(server, null);
    try {
      while (handleRequest(client, in, out)) { }
    } finally {
      in.close();
      out.close();
    }
  }

  public interface Connection {
    public InputStream execute(String command);
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
