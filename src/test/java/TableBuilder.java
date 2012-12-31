/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package unittests;

import junit.framework.TestCase;

import org.junit.Test;
import static com.readytalk.revori.util.Util.cols;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;

import com.readytalk.revori.Column;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;

public class TableBuilder extends TestCase {
  @Test
  public void testColumn() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.table(numbers)
      .row(1).update(name, "one")
      .row(2).update(name, "two")
      .row(3).update(name, "three")
      .row(4).update(name, "four")
      .row(5).update(name, "five")
      .row(6).update(name, "six")
      .row(7).update(name, "seven")
      .row(8).update(name, "eight")
      .row(9).update(name, "nine");

    Revision first = builder.commit();

    assertEquals("one", first.query(numbers.primaryKey, 1, name));
    assertEquals("two", first.query(numbers.primaryKey, 2, name));
    assertEquals("three", first.query(numbers.primaryKey, 3, name));
    assertEquals("four", first.query(numbers.primaryKey, 4, name));
    assertEquals("five", first.query(numbers.primaryKey, 5, name));
    assertEquals("six", first.query(numbers.primaryKey, 6, name));
    assertEquals("seven", first.query(numbers.primaryKey, 7, name));
    assertEquals("eight", first.query(numbers.primaryKey, 8, name));
    assertEquals("nine", first.query(numbers.primaryKey, 9, name));
    
    
    assertEquals(null, first.query(numbers.primaryKey, 10, name));
  }

  @Test
  public void testKey() {
    Column<Double> number = new Column<Double>(Double.class);
    Table awesomeNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.table(awesomeNumbers)
      .key(2 * Math.PI)
      .key(Math.E)
      .key(6.0)
      .key(7.0)
      .key(28.0);

    Revision first = builder.commit();

    assertEquals(2 * Math.PI, first.query(awesomeNumbers.primaryKey, 2 * Math.PI, number));
    assertEquals(Math.E, first.query(awesomeNumbers.primaryKey, Math.E, number));
    assertEquals(6.0, first.query(awesomeNumbers.primaryKey, 6.0, number));
    assertEquals(7.0, first.query(awesomeNumbers.primaryKey, 7.0, number));
    assertEquals(28.0, first.query(awesomeNumbers.primaryKey, 28.0, number));
    
    // 10 is decidedly non-awesome
    assertEquals(null, first.query(awesomeNumbers.primaryKey, 10.0, number));
  }

  @Test
  public void testDelete() {
    Column<Double> number = new Column<Double>(Double.class);
    Table awesomeNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.table(awesomeNumbers)
      .key(2 * Math.PI)
      .key(Math.E)
      .key(6.0)
      .key(7.0)
      .key(28.0);

    Revision first = builder.commit();
    
    builder = first.builder();
    // sorry, 7!
    builder.table(awesomeNumbers)
      .delete(7.0);
    //sorry, e!
    builder.table(awesomeNumbers)
      .delete(Math.E);
    
    Revision second = builder.commit();

    assertEquals(2 * Math.PI, second.query(awesomeNumbers.primaryKey, 2 * Math.PI, number));
    assertEquals(null, second.query(awesomeNumbers.primaryKey, Math.E, number));
    assertEquals(6.0, second.query(awesomeNumbers.primaryKey, 6.0, number));
    assertEquals(null, second.query(awesomeNumbers.primaryKey, 7.0, number));
    assertEquals(28.0, second.query(awesomeNumbers.primaryKey, 28.0, number));
    
    // 10 is decidedly non-awesome
    assertEquals(null, second.query(awesomeNumbers.primaryKey, 10.0, number));
  }
  
  @Test
  public void testDeleteColumn() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.table(numbers)
      .row(1).update(name, "one")
      .row(2).update(name, "two");

    Revision first = builder.commit();
    
    builder = first.builder();
    builder.table(numbers)
      .row(2).delete(name);
    Revision second = builder.commit();

    assertEquals("one", second.query(numbers.primaryKey, 1, name));
    assertEquals(1, second.query(numbers.primaryKey, 1, number));
    assertEquals(null, second.query(numbers.primaryKey, 2, name));
    assertEquals(2, second.query(numbers.primaryKey, 2, number));
  }
  
  @Test
  public void testMultipleTables() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> numberName = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Column<Integer> pointx = new Column<Integer>(Integer.class);
    Column<Integer> pointy = new Column<Integer>(Integer.class);
    Column<String> pointName = new Column<String>(String.class);
    Table points = new Table(cols(pointx, pointy));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder
      .table(numbers)
      .row(1).update(numberName, "one")
      .row(2).update(numberName, "two")
      .table(points)
      .row(0, 0).update(pointName, "origin")
      .row(1, 0).update(pointName, "x")
      .row(0, 1).update(pointName, "y")
      .table(numbers)
      .row(3).update(numberName, "three")
      .row(4).update(numberName, "four");

    Revision first = builder.commit();

    assertEquals("one", first.query(numbers.primaryKey, 1, numberName));
    assertEquals("two", first.query(numbers.primaryKey, 2, numberName));
    assertEquals("three", first.query(numbers.primaryKey, 3, numberName));
    assertEquals("four", first.query(numbers.primaryKey, 4, numberName));

    assertEquals("origin", first.query(points.primaryKey, 0, 0, pointName));
    assertEquals("x", first.query(points.primaryKey, 1, 0, pointName));
    assertEquals("y", first.query(points.primaryKey, 0, 1, pointName));
    
  }

}
