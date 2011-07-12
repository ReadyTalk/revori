package unittests;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Overwrite;
import static com.readytalk.oss.dbms.ExpressionFactory.parameter;
import static com.readytalk.oss.dbms.ExpressionFactory.equal;
import static com.readytalk.oss.dbms.ExpressionFactory.reference;

import static org.junit.Assert.assertEquals;

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
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

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

  private static void testDeleteTemplate(boolean restrict) {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

    PatchTemplate englishInsert = new InsertTemplate
      (englishNumbers,
       list(number, name),
       list(parameter(), parameter()),
       Throw);

    PatchTemplate spanishInsert = new InsertTemplate
      (spanishNumbers,
       list(number, name),
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
    Column number = new Column(Integer.class);
    Column english = new Column(String.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(english));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(english),
                               englishNumbers, list(name)));

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
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    try {
      builder.commit();
      fail("expected ForeignKeyException");
    } catch (ForeignKeyException e) { }
  }

  private static void testMerge(boolean restrict) {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

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
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    builder = builder.commit().builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

    try {
      builder.commit();
      fail("expected ForeignKeyException");
    } catch (ForeignKeyException e) { }
  }

  @Test
  public void testDiff() {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = Revisions.Empty.builder();

    builder.add(new ForeignKey(spanishNumbers, list(number),
                               englishNumbers, list(number)));

    builder.insert(Throw, englishNumbers, 1, name, "one");
    builder.insert(Throw, spanishNumbers, 1, name, "uno");
    builder.insert(Throw, englishNumbers, 2, name, "two");
    builder.insert(Throw, spanishNumbers, 2, name, "dos");

    Revision base = builder.commit();

    builder = base.builder();

    builder.delete(englishNumbers, 1);

    Revision fork = builder.commit(ForeignKeyResolvers.Delete);
    
    for (Boolean skipBrokenReferences: list(true, false)) {
      System.out.println("skipBrokenReferences " + skipBrokenReferences);

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
  }
}
