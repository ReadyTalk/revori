import static com.readytalk.oss.dbms.imp.Util.list;
import static com.readytalk.oss.dbms.imp.Util.set;

import com.readytalk.oss.dbms.DBMS;
import com.readytalk.oss.dbms.DBMS.ColumnType;
import com.readytalk.oss.dbms.DBMS.Column;
import com.readytalk.oss.dbms.DBMS.Index;
import com.readytalk.oss.dbms.DBMS.Table;
import com.readytalk.oss.dbms.DBMS.Expression;
import com.readytalk.oss.dbms.DBMS.Revision;
import com.readytalk.oss.dbms.DBMS.PatchContext;
import com.readytalk.oss.dbms.DBMS.PatchTemplate;
import com.readytalk.oss.dbms.DBMS.TableReference;
import com.readytalk.oss.dbms.DBMS.BinaryOperationType;
import com.readytalk.oss.dbms.DBMS.QueryTemplate;
import com.readytalk.oss.dbms.DBMS.QueryResult;
import com.readytalk.oss.dbms.DBMS.ResultType;
import com.readytalk.oss.dbms.imp.MyDBMS;

import java.util.Set;
import java.util.Collections;

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

  private static void testSimpleInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
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
             dbms.constant("forty two"))));

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testLargerInsertDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()));

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate, 42, "forty two");
    dbms.apply(context, insertTemplate, 43, "forty three");
    dbms.apply(context, insertTemplate, 44, "forty four");
    dbms.apply(context, insertTemplate,  2, "two");
    dbms.apply(context, insertTemplate, 65, "sixty five");
    dbms.apply(context, insertTemplate,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(tail, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, insertTemplate, 1, "one");
    dbms.apply(context, insertTemplate, 3, "three");
    dbms.apply(context, insertTemplate, 5, "five");
    dbms.apply(context, insertTemplate, 6, "six");

    Revision second = dbms.commit(context);

    result = dbms.diff(tail, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, first, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, queryTemplate, 5);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "five");
    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testDeleteDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()));

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate, 42, "forty two");
    dbms.apply(context, insertTemplate, 43, "forty three");
    dbms.apply(context, insertTemplate, 44, "forty four");
    dbms.apply(context, insertTemplate,  2, "two");
    dbms.apply(context, insertTemplate, 65, "sixty five");
    dbms.apply(context, insertTemplate,  8, "eight");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);
    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    PatchTemplate deleteTemplate = dbms.deleteTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, deleteTemplate, 43);

    Revision second = dbms.commit(context);

    QueryResult result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(tail, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, tail, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(first);

    dbms.apply(context, deleteTemplate, 43);
    dbms.apply(context, deleteTemplate, 42);
    dbms.apply(context, deleteTemplate, 65);

    second = dbms.commit(context);

    result = dbms.diff(first, second, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 43);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty three");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty two");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 65);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 65);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "sixty five");
    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(first, second, queryTemplate, 2);

    expectEqual(result.nextRow(), ResultType.End);
    
    result = dbms.diff(second, first, queryTemplate, 2);

    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(second);

    dbms.apply(context, deleteTemplate, 44);
    dbms.apply(context, deleteTemplate,  2);
    dbms.apply(context, deleteTemplate,  8);

    Revision third = dbms.commit(context);

    result = dbms.diff(second, third, queryTemplate, 44);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "forty four");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(third, second, queryTemplate, 44);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "forty four");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, queryTemplate, 44);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, third, queryTemplate, 42);

    expectEqual(result.nextRow(), ResultType.End);
  }

  private static void testUpdateDiffs() {
    DBMS dbms = new MyDBMS();

    Column number = dbms.column(ColumnType.Integer32);
    Column name = dbms.column(ColumnType.String);
    Table numbers = dbms.table
      (set(number, name),
       dbms.index(list(number), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (numbers,
       list(number, name),
       list(dbms.parameter(),
            dbms.parameter()));

    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate,  1, "one");
    dbms.apply(context, insertTemplate,  2, "two");
    dbms.apply(context, insertTemplate,  3, "three");
    dbms.apply(context, insertTemplate,  4, "four");
    dbms.apply(context, insertTemplate,  5, "five");
    dbms.apply(context, insertTemplate,  6, "six");
    dbms.apply(context, insertTemplate,  7, "seven");
    dbms.apply(context, insertTemplate,  8, "eight");
    dbms.apply(context, insertTemplate,  9, "nine");
    dbms.apply(context, insertTemplate, 10, "ten");
    dbms.apply(context, insertTemplate, 11, "eleven");
    dbms.apply(context, insertTemplate, 12, "twelve");
    dbms.apply(context, insertTemplate, 13, "thirteen");

    Revision first = dbms.commit(context);

    TableReference numbersReference = dbms.tableReference(numbers);

    PatchTemplate updateTemplate = dbms.updateTemplate
      (numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()),
       list(name),
       list(dbms.parameter()));

    context = dbms.patchContext(first);

    dbms.apply(context, updateTemplate, 1, "ichi");

    Revision second = dbms.commit(context);

    QueryTemplate queryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(numbersReference, number),
        dbms.parameter()));

    QueryResult result = dbms.diff(first, second, queryTemplate, 1);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(second, first, queryTemplate, 1);

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, second, queryTemplate, 2);

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, queryTemplate, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "one");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, second, queryTemplate, 1);

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "ichi");
    expectEqual(result.nextRow(), ResultType.End);

    context = dbms.patchContext(second);

    dbms.apply(context, updateTemplate, 11, "ju ichi");
    dbms.apply(context, updateTemplate,  6, "roku");
    dbms.apply(context, updateTemplate,  7, "shichi");

    Revision third = dbms.commit(context);

    QueryTemplate unconditionalQueryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(numbersReference, name)),
       numbersReference,
       dbms.constant(true));

    result = dbms.diff(second, third, unconditionalQueryTemplate);

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

    Column country = dbms.column(ColumnType.String);
    Column state = dbms.column(ColumnType.String);
    Column city = dbms.column(ColumnType.String);
    Column zip = dbms.column(ColumnType.Integer32);
    Column color = dbms.column(ColumnType.String);
    Table places = dbms.table
      (set(country, state, city, zip, color),
       dbms.index(list(country, state, city), true),
       EmptyIndexSet);

    Revision tail = dbms.revision();

    PatchTemplate insertTemplate = dbms.insertTemplate
      (places,
       list(country, state, city, zip, color),
       list(dbms.parameter(),
            dbms.parameter(),
            dbms.parameter(),
            dbms.parameter(),
            dbms.parameter()));
    
    PatchContext context = dbms.patchContext(tail);

    dbms.apply(context, insertTemplate,
               "USA", "Colorado", "Denver", 80209, "teal");
    dbms.apply(context, insertTemplate,
               "USA", "Colorado", "Glenwood Springs", 81601, "orange");
    dbms.apply(context, insertTemplate,
               "USA", "New York", "New York", 10001, "blue");
    dbms.apply(context, insertTemplate,
               "France", "N/A", "Paris", 0, "pink");
    dbms.apply(context, insertTemplate,
               "England", "N/A", "London", 0, "red");
    dbms.apply(context, insertTemplate,
               "China", "N/A", "Beijing", 0, "red");
    dbms.apply(context, insertTemplate,
               "China", "N/A", "Shanghai", 0, "green");

    Revision first = dbms.commit(context);

    TableReference placesReference = dbms.tableReference(places);

    QueryTemplate stateQueryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(placesReference, color),
            (Expression) dbms.columnReference(placesReference, zip)),
       placesReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(placesReference, state),
        dbms.parameter()));

    QueryResult result = dbms.diff
      (tail, first, stateQueryTemplate, "Colorado");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "teal");
    expectEqual(result.nextItem(), 80209);
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "orange");
    expectEqual(result.nextItem(), 81601);
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(first, tail, stateQueryTemplate, "Colorado");

    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "teal");
    expectEqual(result.nextItem(), 80209);
    expectEqual(result.nextRow(), ResultType.Deleted);
    expectEqual(result.nextItem(), "orange");
    expectEqual(result.nextItem(), 81601);
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, stateQueryTemplate, "N/A");

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

    QueryTemplate countryQueryTemplate = dbms.queryTemplate
      (list((Expression) dbms.columnReference(placesReference, color),
            (Expression) dbms.columnReference(placesReference, city)),
       placesReference,
       dbms.operation
       (BinaryOperationType.Equal,
        dbms.columnReference(placesReference, country),
        dbms.parameter()));

    result = dbms.diff(tail, first, countryQueryTemplate, "France");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "pink");
    expectEqual(result.nextItem(), "Paris");
    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, countryQueryTemplate, "China");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "red");
    expectEqual(result.nextItem(), "Beijing");
    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "green");
    expectEqual(result.nextItem(), "Shanghai");
    expectEqual(result.nextRow(), ResultType.End);

    QueryTemplate countryStateCityQueryTemplate = dbms.queryTemplate
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

    result = dbms.diff(tail, first, countryStateCityQueryTemplate,
                       "France", "Colorado", "Paris");

    expectEqual(result.nextRow(), ResultType.End);

    result = dbms.diff(tail, first, countryStateCityQueryTemplate,
                       "France", "N/A", "Paris");

    expectEqual(result.nextRow(), ResultType.Inserted);
    expectEqual(result.nextItem(), "pink");
    expectEqual(result.nextItem(), "Paris");
    expectEqual(result.nextRow(), ResultType.End);
  }

  public static void main(String[] args) {
    testSimpleInsertDiffs();

    testLargerInsertDiffs();

    testDeleteDiffs();

    testUpdateDiffs();

    testMultilevelIndexes();
  }
}
