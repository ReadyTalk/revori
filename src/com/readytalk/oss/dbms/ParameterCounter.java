package com.readytalk.oss.dbms;

import java.util.Set;
import java.util.HashSet;
import java.util.List;

public class ParameterCounter implements ExpressionVisitor {
  public final Set<Parameter> parameters = new HashSet();
  public int count;

  public void visit(Expression e) {
    if (e instanceof Parameter) {
      Parameter pe = (Parameter) e;
      if (parameters.contains(pe)) {
        throw new IllegalArgumentException
          ("duplicate parameter expression");
      } else {
        parameters.add(pe);
        ++ count;
      }
    }
  }

  public static int countParameters(List<Expression> expressions) {
    ParameterCounter counter = new ParameterCounter();
    for (Expression e: expressions) {
      e.visit(counter);
    }
    return counter.count;
  }
}
