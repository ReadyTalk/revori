package com.readytalk.revori.imp;

import com.readytalk.revori.Expression;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;

class ExpressionContext {
  public final Map<Expression, ExpressionAdapter> adapters = new TreeMap();
  public final Set<ColumnReferenceAdapter> columnReferences = new HashSet();
  public final Object[] parameters;
  public final List<ExpressionAdapter> queryExpressions;
  public int parameterIndex;

  public ExpressionContext(Object[] parameters, List<ExpressionAdapter> queryExpressions) {
    this.parameters = parameters;
    this.queryExpressions = queryExpressions;
  }
}
