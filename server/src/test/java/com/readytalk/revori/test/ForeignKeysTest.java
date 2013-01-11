/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;
import static com.readytalk.revori.DuplicateKeyResolution.Overwrite;
import static com.readytalk.revori.DuplicateKeyResolution.Throw;
import static com.readytalk.revori.ExpressionFactory.equal;
import static com.readytalk.revori.ExpressionFactory.parameter;
import static com.readytalk.revori.ExpressionFactory.reference;
import static com.readytalk.revori.util.Util.cols;
import com.google.common.collect.Lists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.readytalk.revori.Column;
import com.readytalk.revori.DeleteTemplate;
import com.readytalk.revori.DiffResult;
import com.readytalk.revori.ForeignKey;
import com.readytalk.revori.ForeignKeyException;
import com.readytalk.revori.ForeignKeyResolvers;
import com.readytalk.revori.InsertTemplate;
import com.readytalk.revori.PatchTemplate;
import com.readytalk.revori.Revision;
import com.readytalk.revori.RevisionBuilder;
import com.readytalk.revori.Revisions;
import com.readytalk.revori.Table;
import com.readytalk.revori.TableReference;

public class ForeignKeysTest {
  private static void testDelete(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));
    Table japaneseNumbers = new Table(cols(number));
    Table binaryNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    builder.add(new ForeignKey(japaneseNumbers, cols(number),
                               spanishNumbers, cols(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, japaneseNumbers, 1, name, "ichi");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");
    builder.insert(Throw, japaneseNumbers, 2, name, "ni");
    builder.insert(Throw, binaryNumbers, 5, name, "101");

    Revision head = builder.commit();

    assertEquals("one", head.query(englishNumbers.primaryKey, 1, name));
    assertEquals("uno", head.query(spanishNumbers.primaryKey, 1, name));
    assertEquals("ichi", head.query(japaneseNumbers.primaryKey, 1, name));
    assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
    assertEquals("dos", head.query(spanishNumbers.primaryKey, 2, name));
    assertEquals("ni", head.query(japaneseNumbers.primaryKey, 2, name));
    assertEquals("101", head.query(binaryNumbers.primaryKey, 5, name));

    builder = head.builder();

    builder.delete(englishNumbers, 1);

    builder.delete(binaryNumbers, 5);
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);

      assertNull(head.query(englishNumbers.primaryKey, 1, name));
      assertNull(head.query(spanishNumbers.primaryKey, 1, name));
      assertNull(head.query(japaneseNumbers.primaryKey, 1, name));
      assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
      assertEquals("dos", head.query(spanishNumbers.primaryKey, 2, name));
      assertEquals("ni", head.query(japaneseNumbers.primaryKey, 2, name));
      assertNull(head.query(binaryNumbers.primaryKey, 5, name));
    }
  }

  @Test
  public void testDelete() {
    testDelete(true);
    testDelete(false);
  }

  private static void testDeleteMulti(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Column<String> binary = new Column<String>(String.class);
    Table firstNumbers = new Table(cols(number, name));
    Table secondNumbers = new Table(cols(name, number, binary));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(secondNumbers, cols(number, name),
                               firstNumbers, cols(number, name)));

    builder.table(firstNumbers).row(1, "one").row(2, "two")
      .table(secondNumbers).row("one", 1, "1").row("two", 2, "10");

    Revision head = builder.commit();

    assertEquals("one", head.query(name, firstNumbers.primaryKey, 1, "one"));
    assertEquals
      (head.query(name, secondNumbers.primaryKey, "one", 1, "1"), "one");
    assertEquals("two", head.query(name, firstNumbers.primaryKey, 2, "two"));
    assertEquals
      (head.query(name, secondNumbers.primaryKey, "two", 2, "10"), "two");

    builder = head.builder();

    builder.delete(firstNumbers, 1);
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);


      assertNull(head.query(name, firstNumbers.primaryKey, 1, "one"));
      assertNull(head.query(name, secondNumbers.primaryKey, "one", 1, "1"));
      assertEquals("two", head.query(name, firstNumbers.primaryKey, 2, "two"));
      assertEquals
        (head.query(name, secondNumbers.primaryKey, "two", 2, "10"), "two");
    }
  }

  @Test
  public void testDeleteMulti() {
    testDeleteMulti(true);
    testDeleteMulti(false);
  }

  public static void testDeleteTemplate(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    PatchTemplate englishInsert = new InsertTemplate
      (englishNumbers,
       cols(number, name),
       Lists.newArrayList(parameter(), parameter()),
       Throw);

    PatchTemplate spanishInsert = new InsertTemplate
      (spanishNumbers,
       cols(number, name),
       Lists.newArrayList(parameter(), parameter()),
       Throw);

    builder.apply(englishInsert, 1, "one");
    builder.apply(spanishInsert, 1, "uno");
    builder.apply(englishInsert, 2, "two");
    builder.apply(spanishInsert, 2, "dos");

    Revision head = builder.commit();

    assertEquals("one", head.query(englishNumbers.primaryKey, 1, name));
    assertEquals("uno", head.query(spanishNumbers.primaryKey, 1, name));
    assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
    assertEquals("dos", head.query(spanishNumbers.primaryKey, 2, name));

    builder = head.builder();

    TableReference englishNumbersReference
      = new TableReference(englishNumbers);

    PatchTemplate englishDelete = new DeleteTemplate
      (englishNumbersReference,
       equal(reference(englishNumbersReference, number),
             parameter()));

    builder.apply(englishDelete, 1);
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);

      assertNull(head.query(englishNumbers.primaryKey, 1, name));
      assertNull(head.query(spanishNumbers.primaryKey, 1, name));
      assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
      assertEquals("dos", head.query(spanishNumbers.primaryKey, 2, name));
    }
  }

  private static void testUpdate(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> english = new Column<String>(String.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(english));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(english),
                               englishNumbers, cols(name)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, "one", name, "uno");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, "two", name, "dos");

    Revision head = builder.commit();

    assertEquals("one", head.query(englishNumbers.primaryKey, 1, name));
    assertEquals("uno", head.query(spanishNumbers.primaryKey, "one", name));
    assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
    assertEquals("dos", head.query(spanishNumbers.primaryKey, "two", name));

    builder = head.builder();

    builder.insert(Overwrite, englishNumbers, 1, name, "ONE");
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);

      assertEquals("ONE", head.query(englishNumbers.primaryKey, 1, name));
      assertNull(head.query(spanishNumbers.primaryKey, "one", name));
      assertNull(head.query(spanishNumbers.primaryKey, "ONE", name));
      assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
      assertEquals("dos", head.query(spanishNumbers.primaryKey, "two", name));
    }
  }

  @Test
  public void testUpdate() {
    testUpdate(true);
    testUpdate(false);
  }

  @Test
  public void testInsert() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    try {
      builder.commit();
      fail("expected ForeignKeyException");
    } catch (ForeignKeyException e) { }
  }

  private static void testMerge(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    Revision base = builder.commit();

    RevisionBuilder leftBuilder = base.builder();

    leftBuilder.delete(englishNumbers, 1);

    Revision left = leftBuilder.commit();

    RevisionBuilder rightBuilder = base.builder();

    rightBuilder.insert(Throw, spanishNumbers, 1, name, "uno");

    Revision right = rightBuilder.commit();

    if (restrict) {
      try {
        base.merge(left, right, null, ForeignKeyResolvers.Restrict);
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      Revision head = base.merge
        (left, right, null, ForeignKeyResolvers.Delete);

      assertNull(head.query(englishNumbers.primaryKey, 1, name));
      assertNull(head.query(spanishNumbers.primaryKey, 1, name));
      assertEquals("two", head.query(englishNumbers.primaryKey, 2, name));
      assertEquals("dos", head.query(spanishNumbers.primaryKey, 2, name));
    }
  }

  @Test
  public void testMerge() {
    testMerge(true);
    testMerge(false);
  }

  @Test
  public void testAddForeignKey() {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    builder = builder.commit().builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    try {
      builder.commit();
      fail("expected ForeignKeyException");
    } catch (ForeignKeyException e) { }
  }

  private static void testDiff(boolean skipBrokenReferences) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number), "english");
    Table spanishNumbers = new Table
      (cols(number), "spanish", Lists.newArrayList(englishNumbers));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    Revision base = builder.commit();

    builder = base.builder();

    builder.delete(englishNumbers, 1);

    Revision fork = builder.commit(ForeignKeyResolvers.Delete);
    
    DiffResult result = base.diff(fork, skipBrokenReferences);

    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(englishNumbers, result.base());
    assertEquals(englishNumbers, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(1, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertEquals(name, result.base());
    assertNull(result.fork());
    assertEquals(DiffResult.Type.Value, result.next());
    assertEquals("one", result.base());
    assertNull(result.fork());

    if (! skipBrokenReferences) {
      assertEquals(DiffResult.Type.Ascend, result.next());
      assertEquals(DiffResult.Type.Ascend, result.next());
      assertEquals(DiffResult.Type.Key, result.next());
      assertEquals(spanishNumbers, result.base());
      assertEquals(spanishNumbers, result.fork());

      assertEquals(DiffResult.Type.Descend, result.next());
      assertEquals(DiffResult.Type.Key, result.next());
      assertEquals(1, result.base());
      assertNull(result.fork());
      assertEquals(DiffResult.Type.Descend, result.next());
      assertEquals(DiffResult.Type.Key, result.next());
      assertEquals(name, result.base());
      assertNull(result.fork());
      assertEquals(DiffResult.Type.Value, result.next());
      assertEquals("uno", result.base());
      assertNull(result.fork());
    }

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.End, result.next());
  }

  @Test
  public void testDiff() {
    testDiff(true);
    testDiff(false);
  }

  @Test
  public void testBlocksAndWindows() {
    Column<String> screenId = new Column<String>(String.class, "screenId");
    Column<Integer> windowId = new Column<Integer>(Integer.class, "windowId");
    Column<Integer> blockId = new Column<Integer>(Integer.class, "blockId");

    Table screens = new Table(cols(screenId), "screens");
    Table windows = new Table
      (cols(screenId, windowId), "windows", Lists.newArrayList(screens));
    Table blocks = new Table
      (cols(screenId, windowId, blockId), "blocks", Lists.newArrayList(windows));

    RevisionBuilder builder = Revisions.Empty.builder();
    builder.add
      (new ForeignKey(windows, cols(screenId), screens, cols(screenId)));
    builder.add
      (new ForeignKey
       (blocks, cols(screenId, windowId), windows, cols(screenId, windowId)));

    Revision base = builder.commit();
    builder = base.builder();

    builder.insert(Throw, screens, "screen 1");
    builder.insert(Throw, windows, "screen 1", 1);
    builder.insert(Throw, blocks, "screen 1", 1, 1);

    DiffResult result = base.diff(builder.commit(), true);

    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(screens, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals("screen 1", result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(windows, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals("screen 1", result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(1, result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(blocks, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals("screen 1", result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(1, result.fork());

    assertEquals(DiffResult.Type.Descend, result.next());
    assertEquals(DiffResult.Type.Key, result.next());
    assertNull(result.base());
    assertEquals(1, result.fork());

    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.Ascend, result.next());
    assertEquals(DiffResult.Type.End, result.next());
  }
}
