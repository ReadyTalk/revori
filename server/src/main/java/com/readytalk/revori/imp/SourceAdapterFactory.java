/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.google.common.collect.ImmutableMap;
import com.readytalk.revori.Join;
import com.readytalk.revori.Source;
import com.readytalk.revori.TableReference;

class SourceAdapterFactory {
	private static final ImmutableMap<Class<? extends Source>, Factory> factories = ImmutableMap
			.<Class<? extends Source>, Factory> builder().put(TableReference.class, new Factory() {
				public SourceAdapter make(Source source) {
					return new TableAdapter((TableReference) source);
				}
			}).put(Join.class, new Factory() {
				public SourceAdapter make(Source source) {
					Join join = (Join) source;
					return new JoinAdapter(join.type, makeAdapter(join.left),
							makeAdapter(join.right));
				}
			}).build();

	public static SourceAdapter makeAdapter(Source source) {
		return factories.get(source.getClass()).make(source);
	}

	private interface Factory {
		public SourceAdapter make(Source source);
	}
}
