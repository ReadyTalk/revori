package com.readytalk.oss.dbms.server;

public class SQLServer {
  private static class ConnectionHandler implements Runnable {
    private final SQLServer server;
    private final SocketChannel channel;

    public ConnectionHandler(SQLServer server,
                             SocketChannel channel)
    {
      this.server = server;
      this.channel = channel;
    }

    public void run() {
      try {
        InputStream in = new BufferedInputStream
          (Channels.newInputStream(channel));

        OutputStream out = new BufferedOutputStream
          (Channels.newOutputStream(channel));

        while (channel.isOpen()) {
          server.handleRequest(in, out);
        }
      } catch (Exception e) {
        log.log(Level.WARNING, null, e);
      } finally {
        channel.close();
      }
    }
  }

  private class ParseResult {
    public final Tree tree;
    public final String next;
    public final List<String> completions;
    public final Task task;

    public ParseResult(Tree tree,
                       String next,
                       List<String> completions,
                       Task task)
    {
      this.tree = tree;
      this.next = next;
      this.completions = completions;
      this.task = task;
    }
  }

  private class ParseContext {
    public List<String> completions;
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
  }

  private static class TreeList implements Tree {
    private final List<Tree> list = new ArrayList();
    
    public void add(Tree tree) {
      list.add(tree);
    }

    public Tree get(int index) {
      return list.get(index);
    }
  }

  private static abstract class Leaf implements Tree {
    public Tree get(int index) {
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
                    OutputStream out);
  }

  private static Source makeSource(SQLServer server,
                                   Tree tree,
                                   List<TableReference> tableReferences,
                                   List<Expression> tests)
  {
    if (tree instanceof Name) {
      Table table = findTable(server, ((Name) tree).value);
      tableReferences.add
        (new TableReference(table, server.dbms.tableReference(table.table)));
    } else if (isTerminal(tree.get(0), "(")) {
      return makeSource(server, tree.get(1), tableRefernences, tests);
    } else {
      tests.add(makeExpression(tree.get(4)));

      return server.dbms.join
        ("left".equals(((Terminal) tree.get(1).get(0)).value)
         ? JoinType.LeftOuter : JoinType.LeftInner,
         makeSource(server, tree.get(0), tableRefernences, tests),
         makeSource(server, tree.get(2), tableRefernences, tests));
    }
  }

  private static Expression makeExpression
    (SQLServer server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    if (tree instanceof Name) {
      return findColumn(tableReferences, null, ((Name) tree).value);
    } else if (tree instanceof StringLiteral) {
      return server.dbms.constant(((StringLiteral) tree).value);
    } else if (tree instanceof NumberLiteral) {
      return server.dbms.constant(((NumberLiteral) tree).value);
    } if (tree.get(0) instanceof Name) {
      return findColumn
        (tableReferences,
         ((Name) tree.get(0)).value,
         ((Name) tree.get(2)).value);
    } else if (tree.get(0) instanceof Terminal) {
      String value = ((Terminal) tree.get(0)).value;
      if ("(".equals(value)) {
        return makeExpression(server, tree.get(1), tableReferences);
      } else {
        return server.dbms.operation
          (findUnaryOperation(server, value), makeExpression
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
    (SQLServer server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    List<Expression> expressions = new ArrayList();
    if (tree instanceof Terminal) {
      DBMS dbms = server.dbms;
      for (TableReference tableReference: tableReferences) {
        for (Column column: tableReference.table.columns) {
          expressions.add
            (dbms.columnReference(tableReference.reference, column.column));
        }
      }
    } else {
      for (Tree expressionTree: (TreeList) tree) {
        expression.add
          (makeExpression(server, expressionTree, tableReferences));
      }
    }
  }

  private static Expression makeExpressionFromWhere
    (SQLServer server,
     Tree tree,
     List<TableReference> tableReferences)
  {
    if (tree == Nothing) {
      return server.dbms.constant(true);
    } else {
      return makeExpression(server, tree.get(1), tableReferences);
    }
  }

  private static Expression andExpressions(SQLServer server,
                                           Expression expression,
                                           List<Expression> expressions)
  {
    for (Expression e: expressions) {
      expression = dbms.operation(BinaryOperationType.And, expression, e);
    }
    return expression;
  }

  private static QueryTemplate makeQueryTemplate(SQLServer server,
                                                 Tree tree,
                                                 int[] expressionCount)
  {
    List<TableReference> tableReferences = new ArrayList();
    List<Expression> tests = new ArrayList();
    Source source = makeSource
      (server, tree.get(3), tableReferences, tests);

    List<Expression> expressions = makeExpressionList
      (server, tree.get(1), tableReferences);

    expressionCount[0] = expressions.size();

    return dbms.queryTemplate
      (expressions, source, andExpressions
       (server, makeExpressionFromWhere
        (server, tree.get(4), tableReferences), tests));
  }

  private static List<DBMS.Column> makeOptionalColumnList(Table table,
                                                          Tree tree)
  {
    if (tree == Nothing) {
      List<DBMS.Column> columns = new ArrayList(table.columns.size());
      for (Column c: table.columns) {
        columns.add(c.column);
      }
      return columns;
    } else {
      return makeColumnList(table, columns.get(1));
    }
  }

  private static PatchTemplate makeInsertTemplate(SQLServer server,
                                                  Tree tree)
  {
    Table table = findTable(server, ((Name) tree.get(2)).value);

    return server.dbms.insertTemplate
      (table.table, makeOptionalColumnList(table, tree.get(3)),
       makeExpressionList(tree.get(6)));
  }

  private static PatchTemplate makeUpdateTemplate(SQLServer server,
                                                  Tree tree)
  {
    Table table = findTable(server, ((Name) tree.get(2)).value);
    TableReference tableReference = new TableReference(table);
    List<TableReference> tableReferences = list(tableReference);

    Tree operations = tree.get(3);
    List<DBMS.Column> columns = new ArrayList();
    List<Expression> values = new ArrayList();
    for (int i = 0; i < tree.length(); ++i) {
      Tree operation = tree.get(i);
      columns.add(makeColumn(table, operation.get(0)));
      values.add(makeExpression(server, operation.get(2), tableReferences));
    }

    return server.dbms.insertTemplate
      (tableReference.reference, makeExpressionFromWhere
       (server, tree.get(4), tableReferences), columns, values);
  }

  private static PatchTemplate makeDeleteTemplate(SQLServer server,
                                                  Tree tree)
  {
    Table table = findTable(server, ((Name) tree.get(2)).value);
    TableReference tableReference = new TableReference(table);
    List<TableReference> tableReferences = list(tableReference);

    return server.dbms.deleteTemplate
      (tableReference.reference, makeExpressionFromWhere
       (server, tree.get(4), tableReferences));
  }

  private static PatchTemplate makeCopyTemplate(SQLServer server,
                                                Tree tree,
                                                int[] expressionCount)
  {
    Table table = findTable(server, ((Name) tree.get(1)).value);

    List<DBMS.Column> columns = makeOptionalColumnList(table, tree.get(2));
    List<Expression> values = new ArrayList(columns.size());
    DBMS dbms = server.dbms;
    for (int i = 0; i < columns.size(); ++i) {
      values.add(dbms.parameter());
    }

    return server.dbms.insertTemplate(table.table, columns, values);
  }

  private static Table makeTable(SQLServer server,
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

    Map<String, Column> columnMap = new HashMap(columns.size());
    Set<DBMS.Column> columnSet = new HashSet(columns.size());
    for (Tree column: columns) {
      DBMS.Column dbmsColumn = dbms.column
        (findColumnType(server, column.get(1)));
      columnSet.add(dbmsColumn);
  
      Column myColumn = new Column(((Name) column.get(0)).value, dbmsColumn);
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

    Table table = new Table
      (((Name) tree.get(2)).value, columnMap, myPrimaryKeyColumns,
       dbms.table
       (columnSet, dbms.index(dbmsPrimaryKeyColumns), EmptyIndexSet));
  }

  private static void diff(DBMS dbms,
                           Revision base,
                           Revision fork,
                           QueryTemplate template,
                           int expressionCount,
                           OutputStream out)
  {
    QueryResult result = dbms.diff(base, fork, template);

    while (true) {
      ResultType resultType = result.nextRow();
      switch (resultType) {
      case Inserted:
        out.write(InsertedRow);
        for (int i = 0; i < expressionCount; ++i) {
          out.write(Item);
          writeString(out, String.valueOf(result.nextItem()));
        }
        break;

      case Deleted:
        out.write(DeletedRow);
        for (int i = 0; i < expressionCount; ++i) {
          out.write(Item);
          writeString(out, String.valueOf(result.nextItem()));
        }
        break;

      case End:
        out.write(End);
        return;

      default:
        throw new RuntimeException("unexpected result type: " + resultType);
      }
    }
  }

  private class ParserFactory {
    public LazyParser expressionParser;
    public LazyParser sourceParser;

    public static Parser parser() {
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
          ParseResult result = parser.parser(context, in);
          if (result.tree != null) {
            result.task = task;
          }
          return result;
        }
      };
    }

    public static ParseResult success(Tree tree, String next) {
      return new ParseResult(tree, next, null, null);
    }

    public static ParseResult fail(List<String> completions) {
      return new ParseResult(null, next, completions, null);
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
            c = charAt(i);
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
            return fail(list(value));
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
                              final boolean addCompletions)
    {
      return new Parser() {
        public ParseResult parse(ParseContext context, String in) {
          String name = parseName(in);
          if (name != null) {
            if (addCompletions) {
              addCompletions(context, type, name);
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
              while (i < in.length()) {
                c = charAt(i);
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
            if (negative) {
              in = next(in, 1);
            }

            int i = 0;
            while (i < in.length()) {
              char c = charAt(i);
              if (Character.isDigit(c)) {
                break;
              } else {
                if (i > 0) {
                  success(new NumberLiteral(negative, in.substring(0, i)),
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
                                terminal("inner"))
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
           {
             DBMS dbms = client.server.dbms;
             int[] expressionCount = new int[1];
             diff(dbms, dbms.revision(), head(client), makeQueryTemplate
                  (client.server, tree, expressionCount), expressionCount[0],
                  out);
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
           {
             DBMS dbms = client.server.dbms;
             int[] expressionCount = new int[1];
             diff(dbms, findTag(client, ((Name) tree.get(1)).value).revision,
                  findTag(client, ((Name) tree.get(2)).value).revision,
                  makeQueryTemplate
                  (client.server, tree.get(3), expressionCount),
                  expressionCount[0], out);
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
           {
             apply(client, makeInsertTemplate(client.server, tree), out);
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
           {
             apply(client, makeUpdateTemplate(client.server, tree), out);
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
           {
             apply(client, makeDeleteTemplate(client.server, tree), out);
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
           {
             int[] expressionCount = new int[1];
             apply(client, makeCopyTemplate
                   (client.server, tree, expressionCount), expressionCount[0],
                   in, out);
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
           {
             pushTransaction(client);

             out.write(Success);
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
           {
             commitTransaction(client);

             out.write(Success);
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
           {
             popTransaction(client);

             out.write(Success);
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
           {
             setTag(client, ((Name) tree.get(1)).value,
                    findTag(client, ((Name) tree.get(2)).value).revision);

             out.write(Success);
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
           {
             ConflictResolver conflictResolver = new ConflictResolver();
             setTag(client, "head",
                    client.server.dbms.merge
                    (findTag(client, ((Name) tree.get(1)).value).revision,
                     findTag(client, ((Name) tree.get(2)).value).revision,
                     findTag(client, ((Name) tree.get(3)).value).revision,
                     conflictResolver));

             out.write(Success);
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
           {
             out.write(Success);
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
           {
             tree = tree.get(1);
             String type = ((Terminal) tree.get(0)).value;
             String name = ((Name) tree.get(1)).value;

             if ("database".equals(type)) {
               removeDatabase(client, name);
             } else if ("table".equals(type)) {
               removeTable(client, name);
             } else if ("tag".equals(type)) {
               removeTag(client, name);
             } else {
               throw new RuntimeException();
             }

             out.write(Success);
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
           {
             String name = ((Name) tree.get(2)).value;
             client.database = getDatabase(client, name);

             out.write(Success);
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
           {
             setTable(client.server, makeTable(client.server, tree));

             out.write(Success);
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
           {
             String name = ((Name) tree.get(2)).value;
             addDatabase(client, new Database(name));

             out.write(Success);
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
           {
             out.write(Success);
             writeString(out, "todo");
           }
         });
    }
  }

  private static final int ThreadPoolSize = 256;

  private final Parser parser = new ParserFactory().parser();

  private void executeRequest(InputStream in,
                              OutputStream out)
  {
    ParseResult result = parser.parse(readString(in));
    if (result.task != null) {
      try {
        result.task.run(result.tree, in, out);
      } catch (Exception e) {
        out.write(Error);
        writeString(out, e.getMessage()); 
        log.log(Level.WARNING, null, e);       
      }
    } else {
      out.write(Error);
      writeString(out, "Sorry, I don't understand.");
    }
  }

  private void completeRequest(InputStream in,
                               OutputStream out)
  {
    ParseResult result = parser.parse(readString(in));
    out.write(Completions);
    writeInt(out, result.completions.size());
    for (String completion: result.completions) {
      writeString(out, completion);
    }
  }

  private void handleRequest(InputStream in,
                             OutputStream out)
  {
    int requestType = in.read();
    switch (requestType) {
    case Execute:
      executeRequest(in, out);
      out.flush();
      break;

    case Complete:
      completeRequest(in, out);
      out.flush();
      break;

    default:
      throw new RuntimeException("unexpected request type: " + requestType);
    }
  }

  private static void listen(String address,
                             int port)
  {
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(address, port));

    ThreadPoolExecutor executor = new ThreadPoolExecutor
      (ThreadPoolSize,       // core thread count
       ThreadPoolSize,       // maximum thread count
       60, TimeUnit.SECONDS, // maximum thread idle time
       new LinkedBlockingQueue<Runnable>());

    executor.allowCoreThreadTimeOut(true);

    SQLServer server = new SQLServer();

    while (true) {
      SocketChannel channel = server.accept();
      executor.execute(new ConnectionHandler(server, channel));
    }
  }

  public static void main(String[] args) {
    if (args.length == 2) {
      listen(args[0], Integer.parseInt(args[1]));
    } else {
      System.err.println("usage: java " + SQLServer.class.getName()
                         + " <address> <port>");
      System.exit(-1);
    }
  }
}
