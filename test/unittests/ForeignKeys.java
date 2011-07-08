package unittests;

import static com.readytalk.oss.dbms.util.Util.list;
import static com.readytalk.oss.dbms.DuplicateKeyResolution.Throw;
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
import com.readytalk.oss.dbms.RevisionBuilder;
import com.readytalk.oss.dbms.ForeignKey;
import com.readytalk.oss.dbms.ForeignKeyResolvers;
import com.readytalk.oss.dbms.imp.MyRevision;

public class ForeignKeys extends TestCase {
  @Test
  public void testDeleteCascadeLowLevel() {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = MyRevision.empty().builder();

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
    
    head = builder.commit(ForeignKeyResolvers.Delete);

    assertEquals(head.query(englishNumbers.primaryKey, 1, name), null);
    assertEquals(head.query(spanishNumbers.primaryKey, 1, name), null);
    assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
    assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");
  }

  @Test
  public void testDeleteCascade() {
    Column number = new Column(Integer.class);
    Column name = new Column(String.class);
    Table englishNumbers = new Table(list(number));
    Table spanishNumbers = new Table(list(number));

    RevisionBuilder builder = MyRevision.empty().builder();

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
    
    head = builder.commit(ForeignKeyResolvers.Delete);

    assertEquals(head.query(englishNumbers.primaryKey, 1, name), null);
    assertEquals(head.query(spanishNumbers.primaryKey, 1, name), null);
    assertEquals(head.query(englishNumbers.primaryKey, 2, name), "two");
    assertEquals(head.query(spanishNumbers.primaryKey, 2, name), "dos");
  }

  @Test
  public void testUpdateCascadeLowLevel() {
    // todo: test updates using MyRevisionBuilder.insert(Overwrite, ...)
  }

  @Test
  public void testUpdateCascade() {
    // todo: test updates using UpdateTemplateAdapter
  }

  @Test
  public void testInsertLowLevel() {
    // todo: test updates using MyRevisionBuilder.insert(Throw, ...)
  }

  @Test
  public void testInsert() {
    // todo: test updates using InsertTemplateAdapter
  }

  @Test
  public void testMerge() {
    // todo
  }

  @Test
  public void testDiff() {
    // todo
  }
}
