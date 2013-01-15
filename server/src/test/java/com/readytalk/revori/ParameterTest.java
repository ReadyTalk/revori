package com.readytalk.revori;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class ParameterTest {
	
	private Parameter param;
	
	@Before
	public void setUp() throws Exception {
		param = new Parameter();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void hasNulLType() {
		assertNull(param.typeConstraint());
	}
	
	@Test
	public void noChildren() {
		assertTrue(Iterables.isEmpty(param.children()));
	}
	
	@Test
	public void isAsymptotic() {
		Parameter param2 = new Parameter();
		
		assertTrue(param2.compareTo(param) > 0);
		assertTrue(param.compareTo(param2) < 0);
	}
	
	@Test
	public void visitVisits() {
		ExpressionVisitor visitor = mock(ExpressionVisitor.class);
		param.visit(visitor);
		
		verify(visitor).visit(param);
	}
	
	@Test
	public void equalsSelfIsTrue() {
		assertTrue(param.equals(param));
	}
	
	@Test
	public void equalsNextIsTrue() {
		Parameter param2 = new Parameter();
		
		assertFalse(param.equals(param2));
	}
	
	@Test
	public void equalsNullIsFalse() {
		assertFalse(param.equals(null));
	}
	
	@Test
	public void hashCodeDoesNotChange() {
		assertEquals(param.hashCode(), param.hashCode());
	}
	
	@Test
	public void hashCodeCanChange() {
		Parameter param2 = new Parameter();
		
		assertNotEquals(param.hashCode(), param2.hashCode());
	}

}
