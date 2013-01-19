package com.readytalk.revori.server.simple;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.readytalk.revori.ConflictResolver;
import com.readytalk.revori.ForeignKeyResolver;
import com.readytalk.revori.Revision;
import com.readytalk.revori.Revisions;

public class SimpleRevisionServerTest {

	private final ConflictResolver conflictResolver = mock(ConflictResolver.class);
	private final ForeignKeyResolver foreignKeyResolver = mock(ForeignKeyResolver.class);
	private final Revision revision = mock(Revision.class);
	private final Runnable listener = mock(Runnable.class);

	private SimpleRevisionServer server;

	@Before
	public void setUp() throws Exception {
		reset(conflictResolver, foreignKeyResolver, revision, listener);

		server = new SimpleRevisionServer(conflictResolver, foreignKeyResolver);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void unsetHeadIsEmpty() {
		assertEquals(Revisions.Empty, server.head());
	}

	@Test
	public void mergeSameDoesNothing() {
		server.merge(Revisions.Empty, Revisions.Empty);

		verify(revision, never()).merge(any(Revision.class),
				any(Revision.class), any(ConflictResolver.class),
				any(ForeignKeyResolver.class));
	}
	
	@Test
	public void registerListenerRunsListener() {
		server.registerListener(listener);

		verify(listener, times(1)).run();
	}
	
	@Test
	public void registerListenerAddsListener() {
		server.registerListener(listener);
		
		reset(listener);

		server.notifyListeners();
		
		verify(listener, times(1)).run();
	}
	
	@Test
	public void registerListenerReturnsUnregister() {
		server.registerListener(listener).cancel();
		
		reset(listener);
		
		server.notifyListeners();
		
		verify(listener, never()).run();
	}
	
	@Test
	public void mergePerformsReplacement() {
		
		Revision revised = mock(Revision.class);
		Revision fork = mock(Revision.class);
		
		server.registerListener(listener);
		reset(listener);
		
		when(revision.merge(eq(Revisions.Empty),
				eq(fork), any(ConflictResolver.class),
				any(ForeignKeyResolver.class))).thenReturn(revised);
		
		server.merge(revision, fork);
		
		
		assertEquals(revised, server.head());
		verify(listener, times(1)).run();
	}

}
