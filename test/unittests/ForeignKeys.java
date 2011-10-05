package unittests;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.util.Util.cols;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Overwrite;
import static com.readytalk.oss.dbms.ExpressionFactory.parameter;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;

import org.junit.Test;

import junit.framework.TestCase;

import com.readytalk.oss.dbms.Column;
import com.readytalk.oss.dbms.Table;
import com.readytalk.oss.dbms.TableReference;
import com.readytalk.oss.dbms.PatchTemplate;
import com.readytalk.oss.dbms.InsertTemplate;
import com.readytalk.oss.dbms.DeleteTemplate;
import com.readytalk.oss.dbms.Revision;
import com.readytalk.oss.dbms.Revisions;
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.DiffResult;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyResolvers;
import com.readytalk.oss.dbms.ForeignKeyException;

public class ForeignKeys extends TestCase {
  private static void testDelete(boolean restrict) {
    Column<Integer> number = new Column<Integer>(Integer.class);
    Column<String> name = new Column<String>(String.class);
    Table englishNumbers = new Table(cols(number));
    Table spanishNumbers = new Table(cols(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, cols(number),
                               englishNumbers, cols(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    Revision head = builder.commit();

    assertEquals(head.query(englishNumbers.primaryKey, 1, name), "one");
    assertEquals(head.query(spanishNumbers.primaryKey, 1, name), "uno");
    assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
    assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");

    builder = head.builder();

    builder.delete(englishNumbers, 1);
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);

      assertEquals(head.query(englishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(spanishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
      assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");
    }
  }

  @Test
  public void testDelete() {
    testDelete(true);
    testDelete(false);
  }

  @Test
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
       list(parameter(), parameter()),
       Throw);

    PatchTemplate spanishInsert = new InsertTemplate
      (spanishNumbers,
       cols(number, name),
       list(parameter(), parameter()),
       Throw);

    builder.apply(englishInsert, 1, "one");
    builder.apply(spanishInsert, 1, "uno");
    builder.apply(englishInsert, 2, "two");
    builder.apply(spanishInsert, 2, "dos");

    Revision head = builder.commit();

    assertEquals(head.query(englishNumbers.primaryKey, 1, name), "one");
    assertEquals(head.query(spanishNumbers.primaryKey, 1, name), "uno");
    assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
    assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");

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

      assertEquals(head.query(englishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(spanishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
      assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");
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

    assertEquals(head.query(englishNumbers.primaryKey, 1, name), "one");
    assertEquals(head.query(spanishNumbers.primaryKey, "one", name), "uno");
    assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
    assertEquals(head.query(spanishNumbers.primaryKey, "two", name), "dos");

    builder = head.builder();

    builder.insert(Overwrite, englishNumbers, 1, name, "ONE");
    
    if (restrict) {
      try {
        builder.commit();
        fail("expected ForeignKeyException");
      } catch (ForeignKeyException e) { }
    } else {
      head = builder.commit(ForeignKeyResolvers.Delete);

      assertEquals(head.query(englishNumbers.primaryKey, 1, name), "ONE");
      assertEquals(head.query(spanishNumbers.primaryKey, "one", name), null);
      assertEquals(head.query(spanishNumbers.primaryKey, "ONE", name), null);
      assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
      assertEquals(head.query(spanishNumbers.primaryKey, "two", name), "dos");
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

      assertEquals(head.query(englishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(spanishNumbers.primaryKey, 1, name), null);
      assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
      assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");
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
      (cols(number), "spanish", list(englishNumbers));

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

    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), englishNumbers);
    assertEquals(result.fork(), englishNumbers);

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), 1);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), name);
    assertEquals(result.fork(), null);
    assertEquals(result.next(), DiffResult.Type.Value);
    assertEquals(result.base(), "one");
    assertEquals(result.fork(), null);

    if (! skipBrokenReferences) {
      assertEquals(result.next(), DiffResult.Type.Ascend);
      assertEquals(result.next(), DiffResult.Type.Ascend);
      assertEquals(result.next(), DiffResult.Type.Key);
      assertEquals(result.base(), spanishNumbers);
      assertEquals(result.fork(), spanishNumbers);

      assertEquals(result.next(), DiffResult.Type.Descend);
      assertEquals(result.next(), DiffResult.Type.Key);
      assertEquals(result.base(), 1);
      assertEquals(result.fork(), null);
      assertEquals(result.next(), DiffResult.Type.Descend);
      assertEquals(result.next(), DiffResult.Type.Key);
      assertEquals(result.base(), name);
      assertEquals(result.fork(), null);
      assertEquals(result.next(), DiffResult.Type.Value);
      assertEquals(result.base(), "uno");
      assertEquals(result.fork(), null);
    }

    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.End);
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
      (cols(screenId, windowId), "windows", list(screens));
    Table blocks = new Table
      (cols(screenId, windowId, blockId), "blocks", list(windows));

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

    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), screens);

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "screen 1");

    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), windows);

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "screen 1");

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 1);

    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), blocks);

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), "screen 1");

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 1);

    assertEquals(result.next(), DiffResult.Type.Descend);
    assertEquals(result.next(), DiffResult.Type.Key);
    assertEquals(result.base(), null);
    assertEquals(result.fork(), 1);

    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.Ascend);
    assertEquals(result.next(), DiffResult.Type.End);
  }
}
