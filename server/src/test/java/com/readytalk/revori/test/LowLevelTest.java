/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.util.Util.cols;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.DiffResult;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;

public class LowLevelTest {

  @Test
  public void testLowLevelDiffs() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    RevisionBuilder builder = tail.builder();

    builder.insert(Throw, numbers, 1, name, "one");
    builder.insert(Throw, numbers, 2, name, "two");
    builder.insert(Throw, numbers, 3, name, "three");
    builder.insert(Throw, numbers, 4, name, "four");
    builder.insert(Throw, numbers, 5, name, "five");
    builder.insert(Throw, numbers, 6, name, "six");
    builder.insert(Throw, numbers, 7, name, "seven");
    builder.insert(Throw, numbers, 8, name, "eight");
    builder.insert(Throw, numbers, 9, name, "nine");

    Revision first = builder.commit();

    DiffResult result = tail.diff(first, false);

    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(numbers, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(1, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("one", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(2, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("two", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(3, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("three", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(4, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("four", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(5, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("five", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(6, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("six", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(7, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("seven", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(8, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("eight", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(9, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("nine", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.End, result.next());

    builder = first.builder();

    builder.insert(Throw, numbers, 10, name, "ten");
    builder.delete(numbers, 2);
    builder.delete(numbers, 3);
    builder.insert(Overwrite, numbers, 4, name, "cuatro");
    builder.delete(numbers, 5);
    builder.delete(numbers, 35);
    builder.insert(Throw, numbers, 25, name, "twenty-five");

    Revision second = builder.commit();

    result = first.diff(second, false);

    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(numbers, result.base());
    assertEquals(numbers, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(2, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(name, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertEquals("two", result.base());
    assertNull(result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(3, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(name, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertEquals("three", result.base());
    assertNull(result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(4, result.base());
    assertEquals(4, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(name, result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertEquals("four", result.base());
    assertEquals("cuatro", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(5, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(name, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertEquals("five", result.base());
    assertNull(result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(10, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals("ten", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(25, result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(name, result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertNull(result.base());
    assertEquals(result.fork(), "twenty-five");

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.End, result.next());

    { builder = tail.builder();

      builder.insert(Throw, numbers, 1);
      builder.insert(Throw, numbers, 2);

      Revision fork = builder.commit();

      assertEquals(1, fork.query(numbers.primaryKey, 1, number));
      assertEquals(2, fork.query(numbers.primaryKey, 2, number));

      result = tail.diff(fork, false);

      assertEquals(DiffResult.Type.Key, result.next());
      assertNull(result.base());
      assertEquals(numbers, result.fork());

      assertEquals(DiffResult.Type.Descend, result.next());
      assertEquals(DiffResult.Type.Key, result.next());
      assertNull(result.base());
      assertEquals(1, result.fork());

      assertEquals(DiffResult.Type.Key, result.next());
      assertNull(result.base());
      assertEquals(2, result.fork());

      assertEquals(DiffResult.Type.Ascend, result.next());
      assertEquals(DiffResult.Type.End, result.next());
    }
  }

  private static void apply(RevisionBuilder builder, DiffResult result)
  {
    final int MaxDepth = 16;
    Object[] path = new Object[MaxDepth];
    int depth = 0;
    while (true) {
      DiffResult.Type type = result.next();
      switch (type) {
      case End:
        return;

      case Descend:
        ++ depth;
        break;

      case Ascend:
        path[depth--] = null;
        break;

      case Key: {
        Object forkKey = result.fork();
        if (forkKey != null) {
          path[depth] = forkKey;
        } else {
          path[depth] = result.base();          
          builder.delete(path, 0, depth + 1);
          result.skip();
        }
      } break;

      case Value: {
        path[depth + 1] = result.fork();
        builder.insert(Overwrite, path, 0, depth + 2);
      } break;

      default:
        throw new RuntimeException("unexpected result type: " + type);
      }
    }
  }

  @Test
  public void testLowLevelDiffApplication() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table numbers = new Table(cols(number));

    Revision tail = Revisions.Empty;

    RevisionBuilder builder = tail.builder();

    builder.insert(Throw, numbers, 1, name, "one");
    builder.insert(Throw, numbers, 2, name, "two");
    builder.insert(Throw, numbers, 3, name, "three");
    builder.insert(Throw, numbers, 4, name, "four");
    builder.insert(Throw, numbers, 5, name, "five");
    builder.insert(Throw, numbers, 6, name, "six");
    builder.insert(Throw, numbers, 7, name, "seven");
    builder.insert(Throw, numbers, 8, name, "eight");
    builder.insert(Throw, numbers, 9, name, "nine");

    Revision first = builder.commit();

    builder = tail.builder();

    apply(builder, tail.diff(first, false));
    
    Revision firstApplied = builder.commit();
    
    DiffResult result = first.diff(firstApplied, false);
    
    assertEquals(DiffResult.Type.End, result.next());

    builder = first.builder();

    builder.insert(Throw, numbers, 10, name, "ten");
    builder.delete(numbers, 2);
    builder.delete(numbers, 3);
    builder.insert(Overwrite, numbers, 4, name, "cuatro");
    builder.delete(numbers, 5);
    builder.delete(numbers, 35);
    builder.insert(Throw, numbers, 25, name, "twenty-five");

    Revision second = builder.commit();

    builder = first.builder();

    apply(builder, first.diff(second, false));
    
    Revision secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    assertEquals(DiffResult.Type.End, result.next());

    builder = tail.builder();

    apply(builder, tail.diff(second, false));

    secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    assertEquals(DiffResult.Type.End, result.next());

    builder = firstApplied.builder();

    apply(builder, first.diff(second, false));

    secondApplied = builder.commit();
    
    result = second.diff(secondApplied, false);

    assertEquals(DiffResult.Type.End, result.next());
  }
}
