/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

package com.readytalk.revori.imp;

import com.google.common.collect.ImmutableMap;
import com.readytalk.revori.Aggregate;
import com.readytalk.revori.BinaryOperation;
import com.readytalk.revori.ColumnReference;
import com.readytalk.revori.Constant;
import com.readytalk.revori.Expression;
import com.readytalk.revori.Parameter;
import com.readytalk.revori.UnaryOperation;

class ExpressionAdapterFactory {
	private static final ImmutableMap<Class<? extends Expression>, Factory> factories = ImmutableMap
			.<Class<? extends Expression>, Factory> builder()
			.put(Constant.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					return new ConstantAdapter(((Constant) expression).value);
				}
			}).put(Parameter.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					ExpressionAdapter adapter = context.adapters
							.get(expression);
					if (adapter == null) {
						context.adapters
								.put(expression,
										adapter = new ConstantAdapter(
												context.parameters[context.parameterIndex++]));
					}

					return adapter;
				}
			}).put(ColumnReference.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					ColumnReference<?> reference = (ColumnReference<?>) expression;
					ExpressionAdapter adapter = context.adapters.get(reference);
					if (adapter == null) {
						ColumnReferenceAdapter cra = new ColumnReferenceAdapter(
								reference.tableReference, reference.column);
						context.adapters.put(reference, cra);
						context.columnReferences.add(cra);
						return cra;
					} else {
						return adapter;
					}
				}
			}).put(BinaryOperation.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					BinaryOperation operation = (BinaryOperation) expression;
					switch (operation.type.operationClass()) {
					case Comparison:
						return new ComparisonAdapter(operation.type,
								makeAdapter(context, operation.leftOperand),
								makeAdapter(context, operation.rightOperand));

					case Boolean:
						return new BooleanBinaryAdapter(operation.type,
								makeAdapter(context, operation.leftOperand),
								makeAdapter(context, operation.rightOperand));

					default:
						throw new RuntimeException();
					}
				}
			}).put(UnaryOperation.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					UnaryOperation operation = (UnaryOperation) expression;
					switch (operation.type.operationClass()) {
					case Boolean:
						return new BooleanUnaryAdapter(operation.type,
								makeAdapter(context, operation.operand));

					default:
						throw new RuntimeException();
					}
				}
			}).put(Aggregate.class, new Factory() {
				public ExpressionAdapter make(ExpressionContext context,
						Expression expression) {
					ExpressionAdapter adapter = context.adapters
							.get(expression);
					if (adapter == null) {
						context.adapters.put(expression,
								adapter = new AggregateAdapter(
										(Aggregate<?>) expression));
					}
					return adapter;
				}
			}).build();

	public static ExpressionAdapter makeAdapter(ExpressionContext context,
			Expression expression) {
		Factory factory = factories.get(expression.getClass());
		if (factory == null) {
			throw new RuntimeException("no factory found for "
					+ expression.getClass());
		} else {
			return factory.make(context, expression);
		}
	}

	private interface Factory {
		public ExpressionAdapter make(ExpressionContext context,
				Expression expression);
	}
}
