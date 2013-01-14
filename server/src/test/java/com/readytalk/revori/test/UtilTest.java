/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.test;

import com.readytalk.revori.util.Util;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest {
  @Test
  public void simpleLong() {
    String input = "12";
    Object output = Util.convert(Long.class, input);
    Assert.assertTrue("Return type must be a long", output instanceof Long);
    long value = (Long)output;
    Assert.assertEquals("Must equal 12", 12, value);
  }

  @Test
  public void longWithLeadingWhitespace() {
    String input = " 12";
    Object output = Util.convert(Long.class, input);
    Assert.assertTrue("Return type must be a long", output instanceof Long);
    long value = (Long)output;
    Assert.assertEquals("Must equal 12", 12, value);
  }
}
