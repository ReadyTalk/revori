package com.readytalk.revori;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class ColumnReferenceTest {

	private final TableReference table = mock(TableReference.class);

	@SuppressWarnings("unchecked")
	private final Column<Integer> col = mock(Column.class);

	private ColumnReference<Integer> colRef;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		reset(table, col);

		colRef = new ColumnReference<Integer>(table, col);
		
		when(table.compareTo(eq(table))).thenReturn(0);
		when(col.compareTo(eq(col))).thenReturn(0);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void visitVisits() {
		ExpressionVisitor visitor = mock(ExpressionVisitor.class);
		
		colRef.visit(visitor);
		
		verify(visitor).visit(colRef);
	}
	
	@Test
	public void noChildren() {
		assertTrue(Iterables.isEmpty(colRef.children()));
	}
	

	@Test
	public void equalsNullIsFalse() {
		assertFalse(colRef.equals(null));
	}
	
	@Test
	public void equalsSelfIsTrue() {
		assertTrue(colRef.equals(colRef));
	}
	
	@Test
	public void equalsSameValuesIsTrue() {
		ColumnReference<Integer> colRef2 = new ColumnReference<Integer>(table, col);
		
		assertTrue(colRef.equals(colRef2));
	}
	
	@Test
	public void equalsDifferentTableIsFalse() {
		TableReference table2 = mock(TableReference.class);
		ColumnReference<Integer> colRef2 = new ColumnReference<Integer>(table2, col);
		
		when(table.compareTo(table2)).thenReturn(-1);
		
		assertFalse(colRef.equals(colRef2));
	}
	
	@Test
	public void equalsDifferentColumnIsFalse() {
		@SuppressWarnings("unchecked")
		Column<Integer> col2 = mock(Column.class);
		ColumnReference<Integer> colRef2 = new ColumnReference<Integer>(table, col2);
		
		assertFalse(colRef.equals(colRef2));
	}
	
	@Test
	public void equalObjectsHaveSameHashCode() {
		ColumnReference<Integer> colRef2 = new ColumnReference<Integer>(table, col);
		
		assertEquals(colRef.hashCode(), colRef2.hashCode());
	}
	
	@Test
	public void hashCodeDoesNotChange() {
		assertEquals(colRef.hashCode(), colRef.hashCode());
	}
	
	@Test
	public void hashCodeCanVary() {
		@SuppressWarnings("unchecked")
		Column<Integer> col2 = mock(Column.class);
		ColumnReference<Integer> colRef2 = new ColumnReference<Integer>(table, col2);
		
		assertNotEquals(colRef.hashCode(), colRef2.hashCode());
	}
	
	@Test
	public void differentObjectTypesAreNotEqual() {
		Expression exp = mock(Expression.class);
		
		assertFalse(colRef.equals(exp));
	}

}
