/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package Suites;

import unittests.ColumnTypeTest;
import unittests.DuplicateTest;
import unittests.Epidemic;
import unittests.GeneralTest;
import unittests.GeneralTests;
import unittests.IndexesTest;
import unittests.JoinTest;
import unittests.LowLevel;
import unittests.MergeTest;
import unittests.MultipleIndex;
import unittests.OperationTest;
import unittests.PartialIndexTest;
import unittests.SimpleTest;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

	public static Test suite() {
		
		//$JUnit-BEGIN$		
		TestSuite suite = new TestSuite(ColumnTypeTest.class);
		suite.addTestSuite(DuplicateTest.class);
        suite.addTestSuite(Epidemic.class);
        suite.addTestSuite(GeneralTest.class);
        suite.addTestSuite(GeneralTests.class);
        suite.addTestSuite(IndexesTest.class);
        suite.addTestSuite(JoinTest.class);
        suite.addTestSuite(LowLevel.class);
        suite.addTestSuite(MergeTest.class);
        suite.addTestSuite(MultipleIndex.class);
        suite.addTestSuite(OperationTest.class);
        suite.addTestSuite(PartialIndexTest.class);
        suite.addTestSuite(SimpleTest.class);

		//$JUnit-END$
		return suite;

	}

}
