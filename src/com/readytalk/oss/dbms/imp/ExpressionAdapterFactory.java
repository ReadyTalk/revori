package com.readytalk.oss.dbms.imp;

import com.readytalk.oss.dbms.Expression;
import com.readytalk.oss.dbms.Constant;
import com.readytalk.oss.dbms.Parameter;
import com.readytalk.oss.dbms.ColumnReference;
import com.readytalk.oss.dbms.BinaryOperation;
import com.readytalk.oss.dbms.UnaryOperation;

import java.util.Map;
import java.util.HashMap;

class ExpressionAdapterFactory {
  private static final Map<Class, Factory> factories = new HashMap();

  static {
    factories.put(Constant.class, new Factory() {
      public ExpressionAdapter make(ExpressionContext context,
                                    Expression expression)
      {
        return new ConstantAdapter(((Constant) expression).value);
      }
    });

    factories.put(Parameter.class, new Factory() {
      public ExpressionAdapter make(ExpressionContext context,
                                    Expression expression)
      {
        ExpressionAdapter adapter = context.adapters.get(expression);
        if (adapter == null) {
          context.adapters.put
            (expression, adapter = new ConstantAdapter
             (context.parameters[context.parameterIndex++]));
        }

        return adapter;
      }
    });

    factories.put(ColumnReference.class, new Factory() {
      public ExpressionAdapter make(ExpressionContext context,
                                    Expression expression)
      {
        ColumnReference reference = (ColumnReference) expression;
        ExpressionAdapter adapter = context.adapters.get(reference);
        if (adapter == null) {
          ColumnReferenceAdapter cra = new ColumnReferenceAdapter
            (reference.tableReference, reference.column);
          context.adapters.put(reference, cra);
          context.columnReferences.add(cra);
          return cra;
        } else {
          return adapter;
        }
      }
    });

    factories.put(BinaryOperation.class, new Factory() {
      public ExpressionAdapter make(ExpressionContext context,
                                    Expression expression)
      {
        BinaryOperation operation = (BinaryOperation) expression;
        switch (operation.type.operationClass()) {
        case Comparison:
          return new ComparisonAdapter
            (operation.type,
             makeAdapter(context, operation.leftOperand),
             makeAdapter(context, operation.rightOperand));

        case Boolean:
          return new BooleanBinaryAdapter
            (operation.type,
             makeAdapter(context, operation.leftOperand),
             makeAdapter(context, operation.rightOperand));

        default:
          throw new RuntimeException();
        }
      }
    });

    factories.put(UnaryOperation.class, new Factory() {
      public ExpressionAdapter make(ExpressionContext context,
                                    Expression expression)
      {
        UnaryOperation operation = (UnaryOperation) expression;
        switch (operation.type.operationClass()) {
        case Boolean:
          return new BooleanUnaryAdapter
            (operation.type, makeAdapter(context, operation.operand));

        default:
          throw new RuntimeException();
        }
      }
    });
  }

  public static ExpressionAdapter makeAdapter(ExpressionContext context,
                                              Expression expression)
  {
    Factory factory = factories.get(expression.getClass());
    if (factory == null) {
      throw new RuntimeException
        ("no factory found for " + expression.getClass());
    } else {
      return factory.make(context, expression);
    }
  }

  private interface Factory {
    public ExpressionAdapter make(ExpressionContext context,
                                  Expression expression);
  }
}
