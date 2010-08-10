import static com.readytalk.oss.dbms.imp.Util.list;
import static com.readytalk.oss.dbms.imp.Util.set;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.Index;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.TableReference;
import com.readytalk.oss.dbms.DBMS.UnaryOperationType;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.JoinType;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.DBMS.ConflictResolver;
import com.readytalk.oss.dbms.DBMS.Row;
import com.readytalk.oss.dbms.imp.MyDBMS;

import java.util.Set;
import java.util.Collections;
import java.util.Collection;

public class Test {
  private static final Set<Index> EmptyIndexSet = Collections.emptySet();

  private static void expectEqual(Object value, Object expected) {
    if (value == null) {
      if (expected == null) {
        return;
      }
    } else if (value.equals(expected)) {
      return;
    }
    throw new RuntimeException("expected " + expected + "; got " + value);
  }

  private static void testSimpleInsertQuery() {
    DBMS dbms = new MyDBMS();

    Column key = dbms.column(Integer.class);
    Column firstName = dbms.column(String.class);
    Column lastName = dbms.column(String.class);
    Table names = dbms.table
      (set(key, firstName, lastName),
       dbms.index(list(key), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchContext context = dbms.patchContext(tail);

    PatchTemplate insert = dbms.insertTemplate
      (names,
       list(key, firstName, lastName),
       list(dbms.parameter(),
            dbms.parameter(),
            dbms.parameter()), false);

    dbms.apply(context, insert, 1, "Charles", "Norris");
    dbms.apply(context, insert, 2, "Chuck", "Norris");
    dbms.apply(context, insert, 3, "Chuck", "Taylor");

    Revision first = dbms.commit(context);

    TableReference namesReference = dbms.tableReference(names);

    QueryTemplate any = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, key),
            (Expression) dbms.columnReference(namesReference, firstName),
            (Expression) dbms.columnReference(namesReference, lastName)),
       namesReference,
       dbms.constant(true));

    QueryResult result = dbms.diff(tail, first, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 1);
    expectEqual(result.nextItem(), "Charles");
    expectEqual(result.nextItem(), "Norris");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 2);
    expectEqual(result.nextItem(), "Chuck");
    expectEqual(result.nextItem(), "Norris");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 3);
    expectEqual(result.nextItem(), "Chuck");
    expectEqual(result.nextItem(), "Taylor");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testSimpleInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchContext context = dbms.patchContext(tail);

    dbms.apply
      (context,
       dbms.insertTemplate
       (numbers,
        list(number, name),
        list(dbms.constant(42),
             dbms.constant("forty two")), false));

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate equal = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, equal, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, equal, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, equal, 43);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, first, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testLargerInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert, 42, "forty two");
    dbms.apply(context, insert, 43, "forty three");
    dbms.apply(context, insert, 44, "forty four");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert, 65, "sixty five");
    dbms.apply(context, insert,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate equal = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, equal, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, equal, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, equal, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, first, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, insert, 1, "one");
    dbms.apply(context, insert, 3, "three");
    dbms.apply(context, insert, 5, "five");
    dbms.apply(context, insert, 6, "six");

    Revision second = dbms.commit(context);

    result = dbms.diff(tail, second, equal, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, equal, 5);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, first, equal, 5);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, equal, 5);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testDeleteDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert, 42, "forty two");
    dbms.apply(context, insert, 43, "forty three");
    dbms.apply(context, insert, 44, "forty four");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert, 65, "sixty five");
    dbms.apply(context, insert,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate equal = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    PatchTemplate delete = dbms.deleteTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, delete, 43);

    Revision second = dbms.commit(context);

    QueryResult result = dbms.diff(first, second, equal, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, equal, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, second, equal, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, tail, equal, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, delete, 43);
    dbms.apply(context, delete, 42);
    dbms.apply(context, delete, 65);

    second = dbms.commit(context);

    result = dbms.diff(first, second, equal, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 65);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 65);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, equal, 2);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, equal, 2);

    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(second);

    dbms.apply(context, delete, 44);
    dbms.apply(context, delete,  2);
    dbms.apply(context, delete,  8);

    Revision third = dbms.commit(context);

    result = dbms.diff(second, third, equal, 44);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty four");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(third, second, equal, 44);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty four");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, equal, 44);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, equal, 42);

    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testUpdateDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,  1, "one");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert,  3, "three");
    dbms.apply(context, insert,  4, "four");
    dbms.apply(context, insert,  5, "five");
    dbms.apply(context, insert,  6, "six");
    dbms.apply(context, insert,  7, "seven");
    dbms.apply(context, insert,  8, "eight");
    dbms.apply(context, insert,  9, "nine");
    dbms.apply(context, insert, 10, "ten");
    dbms.apply(context, insert, 11, "eleven");
    dbms.apply(context, insert, 12, "twelve");
    dbms.apply(context, insert, 13, "thirteen");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    PatchTemplate update = dbms.updateTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()),
       list(name),
       list(dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, update, 1, "ichi");

    Revision second = dbms.commit(context);

    QueryTemplate equal = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(first, second, equal, 1);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, equal, 1);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, equal, 2);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, equal, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, equal, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(second);

    dbms.apply(context, update, 11, "ju ichi");
    dbms.apply(context, update,  6, "roku");
    dbms.apply(context, update,  7, "shichi");

    Revision third = dbms.commit(context);

    QueryTemplate any = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.constant(true));

    result = dbms.diff(second, third, any);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "roku");
    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "shichi");
    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ju ichi");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testMultilevelIndexes() {
    DBMS dbms = new MyDBMS();

    Column country = dbms.column(String.class);
    Column state = dbms.column(String.class);
    Column city = dbms.column(String.class);
    Column zip = dbms.column(Integer.class);
    Column color = dbms.column(String.class);
    Table places = dbms.table
      (set(country, state, city, zip, color),
       dbms.index(list(country, state, city), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (places,
       list(country, state, city, zip, color),
       list(dbms.parameter(),
            dbms.parameter(),
            dbms.parameter(),
            dbms.parameter(),
            dbms.parameter()), false);
    
    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,
               "USA", "Colorado", "Denver", 80209, "teal");
    dbms.apply(context, insert,
               "USA", "Colorado", "Glenwood Springs", 81601, "orange");
    dbms.apply(context, insert,
               "USA", "New York", "New York", 10001, "blue");
    dbms.apply(context, insert,
               "France", "N/A", "Paris", 0, "pink");
    dbms.apply(context, insert,
               "England", "N/A", "London", 0, "red");
    dbms.apply(context, insert,
               "China", "N/A", "Beijing", 0, "red");
    dbms.apply(context, insert,
               "China", "N/A", "Shanghai", 0, "green");

    Revision first = dbms.commit(context);

    TableReference placesReference = dbms.tableReference(places);

    QueryTemplate stateEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(placesReference, color),
            (Expression) dbms.columnReference(placesReference, zip)),
       placesReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(placesReference, state),
        dbms.parameter()));

    QueryResult result = dbms.diff
      (tail, first, stateEqual, "Colorado");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "teal");
    expectEqual(result.nextItem(), 80209);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "orange");
    expectEqual(result.nextItem(), 81601);
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, stateEqual, "Colorado");

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "teal");
    expectEqual(result.nextItem(), 80209);
    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "orange");
    expectEqual(result.nextItem(), 81601);
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, stateEqual, "N/A");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "pink");
    expectEqual(result.nextItem(), 0);
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate countryEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(placesReference, color),
            (Expression) dbms.columnReference(placesReference, city)),
       placesReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(placesReference, country),
        dbms.parameter()));

    result = dbms.diff(tail, first, countryEqual, "France");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "pink");
    expectEqual(result.nextItem(), "Paris");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, countryEqual, "China");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), "Beijing");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), "Shanghai");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate countryStateCityEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(placesReference, color),
            (Expression) dbms.columnReference(placesReference, city)),
       placesReference,
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.And,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(placesReference, country),
          dbms.parameter()),
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(placesReference, state),
          dbms.parameter())),
        dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(placesReference, city),
          dbms.parameter())));

    result = dbms.diff(tail, first, countryStateCityEqual,
                       "France", "Colorado", "Paris");

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, countryStateCityEqual,
                       "France", "N/A", "Paris");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "pink");
    expectEqual(result.nextItem(), "Paris");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testComparisons() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,  1, "one");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert,  3, "three");
    dbms.apply(context, insert,  4, "four");
    dbms.apply(context, insert,  5, "five");
    dbms.apply(context, insert,  6, "six");
    dbms.apply(context, insert,  7, "seven");
    dbms.apply(context, insert,  8, "eight");
    dbms.apply(context, insert,  9, "nine");
    dbms.apply(context, insert, 10, "ten");
    dbms.apply(context, insert, 11, "eleven");
    dbms.apply(context, insert, 12, "twelve");
    dbms.apply(context, insert, 13, "thirteen");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate lessThan = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.LessThan,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, lessThan, 1);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThan, 2);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThan, 6);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThan, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate greaterThan = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.GreaterThan,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    result = dbms.diff(tail, first, greaterThan, 13);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThan, 12);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThan, 11);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate lessThanOrEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.LessThanOrEqual,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    result = dbms.diff(tail, first, lessThanOrEqual, 0);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThanOrEqual, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThanOrEqual, 2);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate greaterThanOrEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.GreaterThanOrEqual,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    result = dbms.diff(tail, first, greaterThanOrEqual, 14);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThanOrEqual, 13);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThanOrEqual, 12);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate notEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.NotEqual,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    result = dbms.diff(tail, first, notEqual, 4);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testBooleanOperations() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,  1, "one");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert,  3, "three");
    dbms.apply(context, insert,  4, "four");
    dbms.apply(context, insert,  5, "five");
    dbms.apply(context, insert,  6, "six");
    dbms.apply(context, insert,  7, "seven");
    dbms.apply(context, insert,  8, "eight");
    dbms.apply(context, insert,  9, "nine");
    dbms.apply(context, insert, 10, "ten");
    dbms.apply(context, insert, 11, "eleven");
    dbms.apply(context, insert, 12, "twelve");
    dbms.apply(context, insert, 13, "thirteen");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate greaterThanAndLessThan = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.GreaterThan,
         dbms.columnReference(numbersReference, number),
         dbms.parameter()),
        dbms.operation
        (BinaryOperationType.LessThan,
         dbms.columnReference(numbersReference, number),
         dbms.parameter())));

    QueryResult result = dbms.diff(tail, first, greaterThanAndLessThan, 8, 12);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThanAndLessThan, 8, 8);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, greaterThanAndLessThan, 12, 8);

    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate lessThanOrGreaterThan = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Or,
        dbms.operation
        (BinaryOperationType.LessThan,
         dbms.columnReference(numbersReference, number),
         dbms.parameter()),
        dbms.operation
        (BinaryOperationType.GreaterThan,
         dbms.columnReference(numbersReference, number),
         dbms.parameter())));

    result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 12);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThanOrGreaterThan, 8, 8);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, lessThanOrGreaterThan, 12, 8);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate notEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (UnaryOperationType.Not,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(numbersReference, number),
         dbms.parameter())));

    result = dbms.diff(tail, first, notEqual, 2);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ten");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate greaterThanAndLessThanOrNotLessThanOrEqual
      = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Or,
        dbms.operation
        (BinaryOperationType.And,
         dbms.operation
         (BinaryOperationType.GreaterThan,
          dbms.columnReference(numbersReference, number),
          dbms.parameter()),
         dbms.operation
         (BinaryOperationType.LessThan,
          dbms.columnReference(numbersReference, number),
          dbms.parameter())),
        dbms.operation
        (UnaryOperationType.Not,
         dbms.operation
         (BinaryOperationType.LessThanOrEqual,
          dbms.columnReference(numbersReference, number),
          dbms.parameter()))));

    result = dbms.diff
      (tail, first, greaterThanAndLessThanOrNotLessThanOrEqual, 3, 7, 10);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "six");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "twelve");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testNonIndexedQueries() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,  1, "one");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert,  3, "three");
    dbms.apply(context, insert,  4, "four");
    dbms.apply(context, insert,  5, "five");
    dbms.apply(context, insert,  6, "six");
    dbms.apply(context, insert,  7, "seven");
    dbms.apply(context, insert,  8, "eight");
    dbms.apply(context, insert,  9, "nine");
    dbms.apply(context, insert, 10, "ten");
    dbms.apply(context, insert, 11, "eleven");
    dbms.apply(context, insert, 12, "twelve");
    dbms.apply(context, insert, 13, "thirteen");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate nameEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, number),
            (Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, name),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, nameEqual, "nine");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 9);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, nameEqual, "nine");

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), 9);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate nameLessThan = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, number),
            (Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.LessThan,
        dbms.columnReference(numbersReference, name),
        dbms.parameter()));

    result = dbms.diff(tail, first, nameLessThan, "nine");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 4);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 5);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 8);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), 11);
    expectEqual(result.nextItem(), "eleven");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testSimpleJoins() {
    DBMS dbms = new MyDBMS();

    Column id = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table names = dbms.table
      (set(id, name),
       dbms.index(list(id), true),
       EmptyIndexSet);

    Column nickname = dbms.column(String.class);
    Table nicknames = dbms.table
      (set(id, nickname),
       dbms.index(list(id, nickname), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate nameInsert = dbms.insertTemplate
      (names,
       list(id, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchTemplate nicknameInsert = dbms.insertTemplate
      (nicknames,
       list(id, nickname),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, nameInsert, 1, "tom");
    dbms.apply(context, nameInsert, 2, "ted");
    dbms.apply(context, nameInsert, 3, "tim");
    dbms.apply(context, nameInsert, 4, "tod");
    dbms.apply(context, nameInsert, 5, "tes");

    dbms.apply(context, nicknameInsert, 1, "moneybags");
    dbms.apply(context, nicknameInsert, 3, "eight ball");
    dbms.apply(context, nicknameInsert, 4, "baldy");
    dbms.apply(context, nicknameInsert, 5, "knuckles");
    dbms.apply(context, nicknameInsert, 6, "no name");

    Revision first = dbms.commit(context);
   
    TableReference namesReference = dbms.tableReference(names);
    TableReference nicknamesReference = dbms.tableReference(nicknames);

    QueryTemplate namesInnerNicknames = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(nicknamesReference, nickname)),
       dbms.join
       (JoinType.Inner,
        namesReference,
        nicknamesReference),
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(namesReference, id),
        dbms.columnReference(nicknamesReference, id)));
    
    QueryResult result = dbms.diff(tail, first, namesInnerNicknames);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "moneybags");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tes");
    expectEqual(result.nextItem(), "knuckles");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate namesLeftNicknames = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(nicknamesReference, nickname)),
       dbms.join
       (JoinType.LeftOuter,
        namesReference,
        nicknamesReference),
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(namesReference, id),
        dbms.columnReference(nicknamesReference, id)));
    
    result = dbms.diff(tail, first, namesLeftNicknames);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "moneybags");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ted");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tes");
    expectEqual(result.nextItem(), "knuckles");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, nameInsert, 6, "rapunzel");
    dbms.apply(context, nameInsert, 7, "carlos");
    dbms.apply(context, nameInsert, 8, "benjamin");

    dbms.apply(context, nicknameInsert, 1, "big bucks");
    dbms.apply(context, nicknameInsert, 8, "jellybean");

    Revision second = dbms.commit(context);

    result = dbms.diff(first, second, namesLeftNicknames);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "rapunzel");
    expectEqual(result.nextItem(), "no name");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "carlos");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "benjamin");
    expectEqual(result.nextItem(), "jellybean");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, namesLeftNicknames);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "moneybags");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ted");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tes");
    expectEqual(result.nextItem(), "knuckles");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "rapunzel");
    expectEqual(result.nextItem(), "no name");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "carlos");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "benjamin");
    expectEqual(result.nextItem(), "jellybean");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testCompoundJoins() {
    DBMS dbms = new MyDBMS();

    Column id = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table names = dbms.table
      (set(id, name),
       dbms.index(list(id), true),
       EmptyIndexSet);

    Column nickname = dbms.column(String.class);
    Table nicknames = dbms.table
      (set(id, nickname),
       dbms.index(list(id, nickname), true),
       EmptyIndexSet);

    Column lastname = dbms.column(String.class);
    Table lastnames = dbms.table
      (set(name, lastname),
       dbms.index(list(name), true),
       EmptyIndexSet);

    Column string = dbms.column(String.class);
    Column color = dbms.column(String.class);
    Table colors = dbms.table
      (set(string, color),
       dbms.index(list(string), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate nameInsert = dbms.insertTemplate
      (names,
       list(id, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchTemplate nicknameInsert = dbms.insertTemplate
      (nicknames,
       list(id, nickname),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchTemplate lastnameInsert = dbms.insertTemplate
      (lastnames,
       list(name, lastname),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchTemplate colorInsert = dbms.insertTemplate
      (colors,
       list(string, color),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, nameInsert, 1, "tom");
    dbms.apply(context, nameInsert, 2, "ted");
    dbms.apply(context, nameInsert, 3, "tim");
    dbms.apply(context, nameInsert, 4, "tod");
    dbms.apply(context, nameInsert, 5, "tes");

    dbms.apply(context, nicknameInsert, 1, "moneybags");
    dbms.apply(context, nicknameInsert, 1, "big bucks");
    dbms.apply(context, nicknameInsert, 3, "eight ball");
    dbms.apply(context, nicknameInsert, 4, "baldy");
    dbms.apply(context, nicknameInsert, 5, "knuckles");
    dbms.apply(context, nicknameInsert, 6, "no name");

    dbms.apply(context, lastnameInsert, "tom", "thumb");
    dbms.apply(context, lastnameInsert, "ted", "thomson");
    dbms.apply(context, lastnameInsert, "tes", "teasdale");

    dbms.apply(context, colorInsert, "big bucks", "red");
    dbms.apply(context, colorInsert, "baldy", "green");
    dbms.apply(context, colorInsert, "no name", "pink");
    dbms.apply(context, colorInsert, "eight ball", "sky blue");

    Revision first = dbms.commit(context);
   
    TableReference namesReference = dbms.tableReference(names);
    TableReference nicknamesReference = dbms.tableReference(nicknames);
    TableReference lastnamesReference = dbms.tableReference(lastnames);
    TableReference colorsReference = dbms.tableReference(colors);

    QueryTemplate namesInnerNicknamesInnerColors = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(nicknamesReference, nickname),
            (Expression) dbms.columnReference(colorsReference, color)),
       dbms.join
       (JoinType.Inner,
        dbms.join
        (JoinType.Inner,
         namesReference,
         nicknamesReference),
        colorsReference),
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(namesReference, id),
         dbms.columnReference(nicknamesReference, id)),
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(colorsReference, string),
         dbms.columnReference(nicknamesReference, nickname))));
    
    QueryResult result = dbms.diff
      (tail, first, namesInnerNicknamesInnerColors);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextItem(), "sky blue");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate namesLeftNicknamesInnerColors = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(nicknamesReference, nickname),
            (Expression) dbms.columnReference(colorsReference, color)),
       dbms.join
       (JoinType.Inner,
        dbms.join
        (JoinType.LeftOuter,
         namesReference,
         nicknamesReference),
        colorsReference),
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(namesReference, id),
         dbms.columnReference(nicknamesReference, id)),
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(colorsReference, string),
         dbms.columnReference(nicknamesReference, nickname))));
    
    result = dbms.diff(tail, first, namesLeftNicknamesInnerColors);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextItem(), "sky blue");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate namesInnerNicknamesLeftColors = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(nicknamesReference, nickname),
            (Expression) dbms.columnReference(colorsReference, color)),
       dbms.join
       (JoinType.LeftOuter,
        dbms.join
        (JoinType.Inner,
         namesReference,
         nicknamesReference),
        colorsReference),
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(namesReference, id),
         dbms.columnReference(nicknamesReference, id)),
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(colorsReference, string),
         dbms.columnReference(nicknamesReference, nickname))));
    
    result = dbms.diff(tail, first, namesInnerNicknamesLeftColors);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "moneybags");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tim");
    expectEqual(result.nextItem(), "eight ball");
    expectEqual(result.nextItem(), "sky blue");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tod");
    expectEqual(result.nextItem(), "baldy");
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tes");
    expectEqual(result.nextItem(), "knuckles");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate namesInnerLastnamesLeftNicknamesLeftColors
       = dbms.queryTemplate
      (list((Expression) dbms.columnReference(namesReference, name),
            (Expression) dbms.columnReference(lastnamesReference, lastname),
            (Expression) dbms.columnReference(nicknamesReference, nickname),
            (Expression) dbms.columnReference(colorsReference, color)),
       dbms.join
       (JoinType.LeftOuter,
        dbms.join
        (JoinType.Inner,
         namesReference,
         lastnamesReference),
        dbms.join
        (JoinType.LeftOuter,
         nicknamesReference,
         colorsReference)),
       dbms.operation
       (BinaryOperationType.And,
        dbms.operation
        (BinaryOperationType.Equal,
         dbms.columnReference(namesReference, name),
         dbms.columnReference(lastnamesReference, name)),
        dbms.operation
        (BinaryOperationType.And,
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(namesReference, id),
          dbms.columnReference(nicknamesReference, id)),
         dbms.operation
         (BinaryOperationType.Equal,
          dbms.columnReference(colorsReference, string),
          dbms.columnReference(nicknamesReference, nickname)))));
    
    result = dbms.diff
      (tail, first, namesInnerLastnamesLeftNicknamesLeftColors);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "thumb");
    expectEqual(result.nextItem(), "big bucks");
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tom");
    expectEqual(result.nextItem(), "thumb");
    expectEqual(result.nextItem(), "moneybags");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ted");
    expectEqual(result.nextItem(), "thomson");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tes");
    expectEqual(result.nextItem(), "teasdale");
    expectEqual(result.nextItem(), "knuckles");
    expectEqual(result.nextItem(), null);
    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void testMerges() {
    DBMS dbms = new MyDBMS();

    final Column number = dbms.column(Integer.class);
    final Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert,  1, "one");
    dbms.apply(context, insert,  2, "two");
    dbms.apply(context, insert,  6, "six");
    dbms.apply(context, insert,  7, "seven");
    dbms.apply(context, insert,  8, "eight");
    dbms.apply(context, insert,  9, "nine");
    dbms.apply(context, insert, 13, "thirteen");

    Revision base = dbms.commit(context);

    context = dbms.patchContext(base);

    dbms.apply(context, insert, 4, "four");

    Revision left = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    PatchTemplate update = dbms.updateTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()),
       list(name),
       list(dbms.parameter()));

    context = dbms.patchContext(base);

    dbms.apply(context, update,  6, "roku");
    dbms.apply(context, insert, 42, "forty two");

    Revision right = dbms.commit(context);

    Revision merge = dbms.merge(base, left, right, null);

    QueryTemplate any = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.constant(true));

    QueryResult result = dbms.diff(tail, merge, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "roku");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "seven");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "nine");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "thirteen");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);
    
    context = dbms.patchContext(base);

    dbms.apply(context, insert, 4, "four");

    left = dbms.commit(context);

    context = dbms.patchContext(base);

    dbms.apply(context, insert, 4, "four");

    right = dbms.commit(context);

    merge = dbms.merge(base, left, right, null);

    result = dbms.diff(base, merge, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "four");
    expectEqual(result.nextRow(), ResultType.End);
    
    PatchTemplate delete = dbms.deleteTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    context = dbms.patchContext(base);

    dbms.apply(context, delete, 8);

    left = dbms.commit(context);

    context = dbms.patchContext(base);

    dbms.apply(context, update, 8, "hachi");

    right = dbms.commit(context);

    merge = dbms.merge(base, left, right, null);

    result = dbms.diff(base, merge, any);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "eight");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(base);

    dbms.apply(context, insert, 4, "four");

    left = dbms.commit(context);

    context = dbms.patchContext(base);

    dbms.apply(context, insert, 4, "shi");

    right = dbms.commit(context);

    merge = dbms.merge(base, left, right, new ConflictResolver() {
        public Row resolveConflict(Table table,
                                   Collection<DBMS.Column> columns,
                                   Revision base,
                                   Row baseRow,
                                   Revision left,
                                   Row leftRow,
                                   Revision right,
                                   Row rightRow)
        {
          expectEqual(baseRow, null);
          expectEqual(leftRow.value(number), 4);
          expectEqual(leftRow.value(name), "four");
          expectEqual(rightRow.value(number), 4);
          expectEqual(rightRow.value(name), "shi");
          
          return new Row() {
            public Object value(Column column) {
              if (column == number) {
                return 4;
              } else if (column == name) {
                return "quatro";
              } else {
                throw new RuntimeException("unexpected column");
              }
            }
          };
        }
      });

    result = dbms.diff(base, merge, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "quatro");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(base);

    dbms.apply(context, update, 1, "ichi");

    left = dbms.commit(context);

    context = dbms.patchContext(base);

    dbms.apply(context, update, 1, "uno");

    right = dbms.commit(context);

    merge = dbms.merge(base, left, right, new ConflictResolver() {
        public Row resolveConflict(Table table,
                                   Collection<DBMS.Column> columns,
                                   Revision base,
                                   Row baseRow,
                                   Revision left,
                                   Row leftRow,
                                   Revision right,
                                   Row rightRow)
        {
          expectEqual(baseRow.value(number), 1);
          expectEqual(baseRow.value(name), "one");
          expectEqual(leftRow.value(number), 1);
          expectEqual(leftRow.value(name), "ichi");
          expectEqual(rightRow.value(number), 1);
          expectEqual(rightRow.value(name), "uno");
          
          return new Row() {
            public Object value(Column column) {
              if (column == number) {
                return 1;
              } else if (column == name) {
                return "unit";
              } else {
                throw new RuntimeException("unexpected column");
              }
            }
          };
        }
      });

    result = dbms.diff(base, merge, any);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "unit");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "one");

    Revision t1 = dbms.commit(context);

    context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "uno");

    Revision t2 = dbms.commit(context);

    merge = dbms.merge(tail, t1, t2, new ConflictResolver() {
        public Row resolveConflict(Table table,
                                   Collection<DBMS.Column> columns,
                                   Revision base,
                                   Row baseRow,
                                   Revision left,
                                   Row leftRow,
                                   Revision right,
                                   Row rightRow)
        {
          expectEqual(baseRow, null);
          expectEqual(leftRow.value(number), 1);
          expectEqual(leftRow.value(name), "one");
          expectEqual(rightRow.value(number), 1);
          expectEqual(rightRow.value(name), "uno");
          
          return new Row() {
            public Object value(Column column) {
              if (column == number) {
                return 1;
              } else if (column == name) {
                return "unit";
              } else {
                throw new RuntimeException("unexpected column");
              }
            }
          };
        }
      });

    result = dbms.diff(tail, merge, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "unit");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "one");
    dbms.apply(context, insert, 2, "two");

    t1 = dbms.commit(context);

    context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "uno");
    dbms.apply(context, insert, 3, "tres");

    t2 = dbms.commit(context);

    merge = dbms.merge(tail, t1, t2, new ConflictResolver() {
        public Row resolveConflict(Table table,
                                   Collection<DBMS.Column> columns,
                                   Revision base,
                                   Row baseRow,
                                   Revision left,
                                   Row leftRow,
                                   Revision right,
                                   Row rightRow)
        {
          expectEqual(baseRow, null);
          expectEqual(leftRow.value(number), 1);
          expectEqual(leftRow.value(name), "one");
          expectEqual(rightRow.value(number), 1);
          expectEqual(rightRow.value(name), "uno");
          
          return new Row() {
            public Object value(Column column) {
              if (column == number) {
                return 1;
              } else if (column == name) {
                return "unit";
              } else {
                throw new RuntimeException("unexpected column");
              }
            }
          };
        }
      });

    result = dbms.diff(tail, merge, any);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "unit");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "two");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "tres");
    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void testUpdateOnPartialIndex() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column color = dbms.column(String.class);
    Column shape = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, color, shape),
       dbms.index(list(number, color), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, color, shape),
       list(dbms.parameter(),
            dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "red", "triangle");
    dbms.apply(context, insert, 1, "green", "circle");
    dbms.apply(context, insert, 2, "yellow", "circle");
    dbms.apply(context, insert, 3, "blue", "square");
    dbms.apply(context, insert, 3, "orange", "square");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate numberEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, color),
            (Expression) dbms.columnReference(numbersReference, shape)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, numberEqual, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), "circle");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), "triangle");
    expectEqual(result.nextRow(), ResultType.End);

    PatchTemplate updateShapeWhereNumberEqual = dbms.updateTemplate
      (numbersReference,
        dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()),
       list(shape),
       list(dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, updateShapeWhereNumberEqual, 1, "pentagon");

    Revision second = dbms.commit(context);

    result = dbms.diff(tail, second, numberEqual, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), "pentagon");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), "pentagon");
    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void testDeleteOnPartialIndex() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column color = dbms.column(String.class);
    Column shape = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, color, shape),
       dbms.index(list(number, color), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, color, shape),
       list(dbms.parameter(),
            dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "red", "triangle");
    dbms.apply(context, insert, 1, "green", "circle");
    dbms.apply(context, insert, 2, "yellow", "circle");
    dbms.apply(context, insert, 3, "blue", "square");
    dbms.apply(context, insert, 3, "orange", "square");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    QueryTemplate numberEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, color),
            (Expression) dbms.columnReference(numbersReference, shape)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, numberEqual, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), "circle");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), "triangle");
    expectEqual(result.nextRow(), ResultType.End);

    PatchTemplate deleteWhereNumberEqual = dbms.deleteTemplate
      (numbersReference,
        dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, deleteWhereNumberEqual, 1);

    Revision second = dbms.commit(context);

    result = dbms.diff(tail, second, numberEqual, 1);

    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void testIndexedColumnUpdates() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(Integer.class);
    Column name = dbms.column(String.class);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insert = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()), false);

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insert, 1, "one");
    dbms.apply(context, insert, 2, "two");
    dbms.apply(context, insert, 3, "three");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    PatchTemplate updateNumberWhereNumberEqual = dbms.updateTemplate
      (numbersReference,
        dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()),
       list(number),
       list(dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, updateNumberWhereNumberEqual, 3, 4);

    Revision second = dbms.commit(context);

    QueryTemplate numberEqual = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, second, numberEqual, 4);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, numberEqual, 3);

    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void main(String[] args) {
    testSimpleInsertQuery();

    testSimpleInsertDiffs();

    testLargerInsertDiffs();

    testDeleteDiffs();

    testUpdateDiffs();

    testMultilevelIndexes();

    testComparisons();

    testBooleanOperations();

    testNonIndexedQueries();

    testSimpleJoins();

    testCompoundJoins();

    testMerges();

    testUpdateOnPartialIndex();

    testDeleteOnPartialIndex();

    testIndexedColumnUpdates();
  }
}
