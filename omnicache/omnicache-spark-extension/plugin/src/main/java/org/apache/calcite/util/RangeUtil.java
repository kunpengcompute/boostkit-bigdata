/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util;

import com.google.common.collect.Range;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.spark.util.Comparison;
import com.huawei.boostkit.spark.util.ExprOptUtil;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import scala.Option;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.NullType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;

public class RangeUtil {
    public static Expression simplifyUsingPredicates(Expression expr, Set<Expression> pulledUpPredicates) {
        Option<Comparison> opt = ExprOptUtil.createComparison(expr);
        if (opt.isEmpty() || opt.get().literal().value() == null) {
            return expr;
        }
        Comparison comparison = opt.get();
        final Comparable v0 = getComparableValueAs(comparison.literal());
        final Range<Comparable> range = range(comparison.kind(), v0);
        final Range<Comparable> range2 = residue(comparison.ref(), range, pulledUpPredicates);
        if (range2 == null) {
            // Term is impossible to satisfy given these predicates
            return Literal.FalseLiteral();
        } else if (range2.equals(range)) {
            // no change
            return expr;
        } else if (range2.equals(Range.all())) {
            // Term is always satisfied given these predicates
            return Literal.TrueLiteral();
        } else if (range2.lowerEndpoint().equals(range2.upperEndpoint())) {
            if (range2.lowerBoundType() == BoundType.OPEN
                    || range2.upperBoundType() == BoundType.OPEN) {
                // range is a point, but does not include its endpoint, therefore is
                // effectively empty
                return Literal.FalseLiteral();
            }
            // range is now a point; it's worth simplifying
            return new EqualTo(comparison.ref(), new Literal(range2.lowerEndpoint(), comparison.literal().dataType()));
        } else {
            // range has been reduced but it's not worth simplifying
            return expr;
        }
    }

    public static Comparable getComparableValueAs(Literal literal) {
        Object value = literal.value();
        Class<Comparable> clazz = Comparable.class;
        if (value == null || clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        throw new AssertionError("cannot convert " + literal.dataType().typeName()
                + " literal to " + clazz);
    }

    private static Range<Comparable> range(Expression kind, Comparable c) {
        switch (kind.prettyName()) {
            case "equalto":
                return Range.singleton(c);
            case "lessthan":
                return Range.lessThan(c);
            case "lessthanorequal":
                return Range.atMost(c);
            case "greaterthan":
                return Range.greaterThan(c);
            case "greaterthanorequal":
                return Range.atLeast(c);
            default:
                throw new AssertionError();
        }
    }

    /**
     * Weakens a term so that it checks only what is not implied by predicates.
     *
     * <p>The term is broken into "ref comparison constant",
     * for example "$0 &lt; 5".
     *
     * <p>Examples:
     * <ul>
     *
     * <li>{@code residue($0 < 10, [$0 < 5])} returns {@code true}
     *
     * <li>{@code residue($0 < 10, [$0 < 20, $0 > 0])} returns {@code $0 < 10}
     * </ul>
     */
    private static Range<Comparable> residue(Expression ref, Range<Comparable> r0, Set<Expression> predicates) {
        for (Expression predicate : predicates) {
            switch (predicate.prettyName()) {
                case "equalto":
                case "lessthan":
                case "lessthanorequal":
                case "greaterthan":
                case "greaterthanorequal":
                    BinaryExpression e = (BinaryExpression) predicate;
                    if (e.left().sql().equals(ref.sql())
                            && e.right() instanceof Literal) {
                        final Literal literal = (Literal) e.right();
                        final Comparable c1 = getComparableValueAs(literal);
                        final Range<Comparable> r1 = range(e, c1);
                        if (r0.encloses(r1)) {
                            // Given these predicates, term is always satisfied.
                            // e.g. r0 is "$0 < 10", r1 is "$0 < 5"
                            return Range.all();
                        }
                        if (r0.isConnected(r1)) {
                            return r0.intersection(r1);
                        }
                        // Ranges do not intersect. Return null meaning the empty range.
                        return null;
                    }
            }
        }
        return r0;
    }

    public static Expression compareLiteral(Literal l1, Literal l2, boolean unknownAsFalse, BinaryExpression condition) {
        final Comparable v0 = getComparableValueAs(l1);
        final Comparable v1 = getComparableValueAs(l2);
        if (v0 == null || v1 == null) {
            return unknownAsFalse ? Literal.FalseLiteral() : new Literal(null, NullType);
        }
        final int comparisonResult = v0.compareTo(v1);
        switch (condition.prettyName()) {
            case "equalto":
                return new Literal(comparisonResult == 0, BooleanType);
            case "greaterthan":
                return new Literal(comparisonResult > 0, BooleanType);
            case "greaterthanorequal":
                return new Literal(comparisonResult >= 0, BooleanType);
            case "lessthan":
                return new Literal(comparisonResult < 0, BooleanType);
            case "lessthanorequal":
                return new Literal(comparisonResult <= 0, BooleanType);
            default:
                throw new AssertionError("Comparison Predicates Required");
        }
    }

    public static Expression processRange(List<Expression> terms,
                                          Map<String, Pair<Range<Comparable>, List<Expression>>> rangeTerms, Expression predicate,
                                          Expression ref, Comparable v0, Expression kind) {
        Pair<Range<Comparable>, List<Expression>> p = rangeTerms.get(ref.sql());
        if (p == null) {
            rangeTerms.put(ref.sql(), Pair.of(range(kind, v0), ImmutableList.of(predicate)));
        } else {
            // Exists
            boolean removeUpperBound = false;
            boolean removeLowerBound = false;
            Range<Comparable> r = p.left;
            switch (kind.prettyName()) {
                case "equalto": {
                    if (!r.contains(v0)) {
                        // Range is empty, not satisfiable
                        return Literal.FalseLiteral();
                    }
                    rangeTerms.put(ref.sql(), Pair.of(Range.singleton(v0), ImmutableList.of(predicate)));
                    // remove
                    for (Expression e : p.right) {
                        replaceAllExpression(terms, e, Literal.TrueLiteral());
                    }
                    break;
                }
                case "lessthan": {
                    int comparisonResult = 0;
                    if (r.hasUpperBound()) {
                        comparisonResult = v0.compareTo(r.upperEndpoint());
                    }
                    if (comparisonResult <= 0) {
                        // 1) No upper bound, or
                        // 2) We need to open the upper bound, or
                        // 3) New upper bound is lower than old upper bound
                        if (r.hasLowerBound()) {
                            if (v0.compareTo(r.lowerEndpoint()) <= 0) {
                                // Range is empty, not satisfiable
                                return Literal.FalseLiteral();
                            }
                            // a <= x < b OR a < x < b
                            r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
                                    v0, BoundType.OPEN);
                        } else {
                            // x < b
                            r = Range.lessThan(v0);
                        }

                        if (r.isEmpty()) {
                            // Range is empty, not satisfiable
                            return Literal.FalseLiteral();
                        }

                        // remove prev upper bound
                        removeUpperBound = true;
                    } else {
                        // Remove this term as it is contained in current upper bound
                        final int index = terms.indexOf(predicate);
                        if (index >= 0) {
                            terms.set(index, Literal.TrueLiteral());
                        }
                    }
                    break;
                }
                case "lessthanorequal": {
                    int comparisonResult = -1;
                    if (r.hasUpperBound()) {
                        comparisonResult = v0.compareTo(r.upperEndpoint());
                    }
                    if (comparisonResult < 0) {
                        // 1) No upper bound, or
                        // 2) New upper bound is lower than old upper bound
                        if (r.hasLowerBound()) {
                            if (v0.compareTo(r.lowerEndpoint()) < 0) {
                                // Range is empty, not satisfiable
                                return Literal.FalseLiteral();
                            }
                            // a <= x <= b OR a < x <= b
                            r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
                                    v0, BoundType.CLOSED);
                        } else {
                            // x <= b
                            r = Range.atMost(v0);
                        }

                        if (r.isEmpty()) {
                            // Range is empty, not satisfiable
                            return Literal.FalseLiteral();
                        }

                        // remove prev upper bound
                        removeUpperBound = true;
                    } else {
                        // Remove this term as it is contained in current upper bound
                        final int index = terms.indexOf(predicate);
                        if (index >= 0) {
                            terms.set(index, Literal.TrueLiteral());
                        }
                    }
                    break;
                }
                case "greaterthan": {
                    int comparisonResult = 0;
                    if (r.hasLowerBound()) {
                        comparisonResult = v0.compareTo(r.lowerEndpoint());
                    }
                    if (comparisonResult >= 0) {
                        // 1) No lower bound, or
                        // 2) We need to open the lower bound, or
                        // 3) New lower bound is greater than old lower bound
                        if (r.hasUpperBound()) {
                            if (v0.compareTo(r.upperEndpoint()) >= 0) {
                                // Range is empty, not satisfiable
                                return Literal.FalseLiteral();
                            }
                            // a < x <= b OR a < x < b
                            r = Range.range(v0, BoundType.OPEN,
                                    r.upperEndpoint(), r.upperBoundType());
                        } else {
                            // x > a
                            r = Range.greaterThan(v0);
                        }

                        if (r.isEmpty()) {
                            // Range is empty, not satisfiable
                            return Literal.FalseLiteral();
                        }

                        // remove prev lower bound
                        removeLowerBound = true;
                    } else {
                        // Remove this term as it is contained in current lower bound
                        final int index = terms.indexOf(predicate);
                        if (index >= 0) {
                            terms.set(index, Literal.TrueLiteral());
                        }
                    }
                    break;
                }
                case "greaterthanorequal": {
                    int comparisonResult = 1;
                    if (r.hasLowerBound()) {
                        comparisonResult = v0.compareTo(r.lowerEndpoint());
                    }
                    if (comparisonResult > 0) {
                        // 1) No lower bound, or
                        // 2) New lower bound is greater than old lower bound
                        if (r.hasUpperBound()) {
                            if (v0.compareTo(r.upperEndpoint()) > 0) {
                                // Range is empty, not satisfiable
                                return Literal.FalseLiteral();
                            }
                            // a <= x <= b OR a <= x < b
                            r = Range.range(v0, BoundType.CLOSED,
                                    r.upperEndpoint(), r.upperBoundType());
                        } else {
                            // x >= a
                            r = Range.atLeast(v0);
                        }

                        if (r.isEmpty()) {
                            // Range is empty, not satisfiable
                            return Literal.FalseLiteral();
                        }

                        // remove prev lower bound
                        removeLowerBound = true;
                    } else {
                        // Remove this term as it is contained in current lower bound
                        final int index = terms.indexOf(predicate);
                        if (index >= 0) {
                            terms.set(index, Literal.TrueLiteral());
                        }
                    }
                    break;
                }
                default:
                    throw new AssertionError();
            }
            if (removeUpperBound) {
                ImmutableList.Builder<Expression> newBounds = ImmutableList.builder();
                for (Expression e : p.right) {
                    if (ExprOptUtil.isUpperBound(e)) {
                        replaceAllExpression(terms, e, Literal.TrueLiteral());
                    } else {
                        newBounds.add(e);
                    }
                }
                newBounds.add(predicate);
                rangeTerms.put(ref.sql(),
                        Pair.of(r, newBounds.build()));
            } else if (removeLowerBound) {
                ImmutableList.Builder<Expression> newBounds = ImmutableList.builder();
                for (Expression e : p.right) {
                    if (ExprOptUtil.isLowerBound(e)) {
                        replaceAllExpression(terms, e, Literal.TrueLiteral());
                    } else {
                        newBounds.add(e);
                    }
                }
                newBounds.add(predicate);
                rangeTerms.put(ref.sql(),
                        Pair.of(r, newBounds.build()));
            }
        }
        // Default
        return null;
    }

    private static boolean replaceAllExpression(List<Expression> terms, Expression oldVal, Expression newVal) {
        boolean result = false;
        for (int i = 0; i < terms.size(); i++) {
            if (terms.get(i).equals(oldVal)) {
                terms.set(i, newVal);
                result = true;
            }
        }
        return result;
    }

    public static void removeExpression(List<Expression> terms, List<Expression> toBeRemoves) {
        for (Expression toBeRemove : toBeRemoves) {
            int index = terms.lastIndexOf(toBeRemove);
            if (index >= 0) {
                terms.remove(index);
            }
        }
    }
}
