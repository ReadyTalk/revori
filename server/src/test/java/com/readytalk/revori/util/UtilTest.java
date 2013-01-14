/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.readytalk.revori.util.Util;

@RunWith(Parameterized.class)
public class UtilTest {

	@Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
				{"-12", -12L},
				{"12", 12L},
				{"2147483667", 2147483667L},
				{"-2147483667", -2147483667L},
		});
	}

	private final String input;
	private final long value;

	public UtilTest(final String input, final long value) {
		this.input = input;
		this.value = value;
	}

	@Test
	public void simpleLong() {
		Object output = Util.convert(Long.class, input);

		long result = (Long) output;
		assertEquals(value, result);
	}

	@Test
	public void longWithLeadingWhitespace() {
		String in = " " + input;
		
		Object output = Util.convert(Long.class, in);

		long result = (Long) output;
		assertEquals(value, result);
	}
	
	@Test
	public void longWithTrailingWhitespace() {
		String in = input + " ";
		
		Object output = Util.convert(Long.class, in);

		long result = (Long) output;
		assertEquals(value, result);
	}
	
	@Test
	public void longWithPadding() {
		String in = " " + input + " ";
		
		Object output = Util.convert(Long.class, in);

		long result = (Long) output;
		assertEquals(value, result);
	}
	
	@Test
	public void simpleInt() {
		Assume.assumeTrue(value <= Integer.MAX_VALUE);
		Assume.assumeTrue(value >= Integer.MIN_VALUE);
		
		Object output = Util.convert(Integer.class, input);

		long result = (Long) output;
		assertEquals(value, result);
	}
}
