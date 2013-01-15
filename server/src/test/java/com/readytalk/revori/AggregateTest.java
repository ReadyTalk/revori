package com.readytalk.revori;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class AggregateTest {
	
	@Rule
	public final ExpectedException thrown = ExpectedException.none();
	
	@SuppressWarnings("unchecked")
	private Foldable<Integer> function = mock(Foldable.class);
	
	private final Expression exp1 = mock(Expression.class);
	private final Expression exp2 = mock(Expression.class);
	
	private Aggregate<Integer> agg;
	
	@Before
	public void setUp() throws Exception {
		reset(function, exp1, exp2);
		
		agg = new Aggregate<Integer>(Integer.class, function, Lists.newArrayList(exp1, exp2));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void orderIncreases() {
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function, Collections.<Expression>emptyList());
		
		assertTrue(agg2.order > agg.order);
	}
	
	@Test
	public void allExpressionsVisited() {
		verify(exp1).visit(any(ExpressionVisitor.class));
		verify(exp2).visit(any(ExpressionVisitor.class));
	}
	
	@Test
	public void immutableExpressionList() {
		thrown.expect(UnsupportedOperationException.class);
		
		agg.expressions.add(exp1);
	}
	
	@Test
	public void visitVisits() {
		ExpressionVisitor visitor = mock(ExpressionVisitor.class);
		
		agg.visit(visitor);
		
		verify(visitor).visit(agg);
	}
	
	@Test
	public void noChildren() {
		assertTrue(Iterables.isEmpty(agg.children()));
	}
	
	
	@Test
	public void equalsNullIsFalse() {
		assertFalse(agg.equals(null));
	}
	
	@Test
	public void equalsSelfIsTrue() {
		assertTrue(agg.equals(agg));
	}
	
	@Test
	public void equalsSameValuesIsTrue() {
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function, Lists.newArrayList(exp1, exp2));
		
		assertTrue(agg.equals(agg2));
	}
	
	@Test
	public void equalsDifferentTypeIsFalse() {
		@SuppressWarnings("unchecked")
		Foldable<String> function2 = mock(Foldable.class);
		
		Aggregate<String> agg2 = new Aggregate<String>(String.class, function2, Lists.newArrayList(exp1, exp2));
		
		assertFalse(agg.equals(agg2));
	}
	
	@Test
	public void equalsDifferentFunctionIsFalse() {
		@SuppressWarnings("unchecked")
		Foldable<Integer> function2 = mock(Foldable.class);
		
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function2, Lists.newArrayList(exp1, exp2));
		
		assertFalse(agg.equals(agg2));
	}
	
	@Test
	public void equalsDifferentExpressionsIsFalse() {
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function, Lists.newArrayList(exp2));
		
		assertFalse(agg.equals(agg2));
	}
	
	@Test
	public void equalObjectsHaveSameHashCode() {
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function, Lists.newArrayList(exp1, exp2));
		
		assertEquals(agg.hashCode(), agg2.hashCode());
	}
	
	@Test
	public void hashCodeDoesNotChange() {
		assertEquals(agg.hashCode(), agg.hashCode());
	}
	
	@Test
	public void hashCodeCanVary() {
		Aggregate<Integer> agg2 = new Aggregate<Integer>(Integer.class, function, Lists.newArrayList(exp2));
		
		assertNotEquals(agg.hashCode(), agg2.hashCode());
	}
	
	@Test
	public void differentObjectTypesAreNotEqual() {
		assertFalse(agg.equals(exp1));
	}

}
