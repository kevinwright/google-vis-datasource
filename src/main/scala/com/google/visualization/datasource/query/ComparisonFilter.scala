// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.visualization.datasource
package query


import datatable.value.Value
import java.{util => ju, lang => jl}
import ju.{StringTokenizer}
import ju.regex.{Pattern, PatternSyntaxException}

/**
 * A filter that decides upon a row by comparing two values. The values can be
 * either column values or constant values, depending on the concrete class
 * extending this one.
 *
 * @author Yonatan B.Y.
 */
abstract class ComparisonFilter(val operator: ComparisonFilterOperator)
extends QueryFilter {

  /**
   * Returns true if s1 is "like" s2, in the sql-sense, i.e., if s2 has any
   * %'s or _'s, they are treated as special characters corresponding to an
   * arbitrary sequence of characters in s1 or to an abitrary character in s1
   * respectively. All other characters in s2 need to match exactly to
   * characters in s1. You cannot escape these characters, so that you cannot
   * match an explicit '%' or '_'.
   * @param s1 The first string.
   * @param s2 The second string.
   * @return True if s1 is "like" s2, in the sql-sense.
   */
  private def isLike(s1: String, s2: String): Boolean = {
    val tokenizer = new StringTokenizer(s2, "%_", true)
    val regexp = new StringBuilder
    while (tokenizer.hasMoreTokens) {
      regexp.append( tokenizer.nextToken match {
        case "%" => ".*"
        case "_" => "."
        case s => Pattern quote s
      })
    }
    s1 matches regexp.toString
  }

  /**
   * Matches the given two values against the operator. E.g., if the operator is
   * GT, returns true if v1 > v2. This implementation uses the
   * compareTo() method.
   *
   * @param v1 The first value.
   * @param v2 The second value.
   *
   * @return true if v1 op v2, false otherwise.
   */
  protected def isOperatorMatch(v1: Value, v2: Value): Boolean = {
    import ComparisonFilterOperator._

    if (operator.areEqualTypesRequired) {
      if (v1.getType != v2.getType) {
        return false
      }
    }
    operator match {
      case EQ => v1.compareTo(v2) == 0
      case NE => v1.compareTo(v2) != 0
      case LT => v1.compareTo(v2) < 0
      case GT => v1.compareTo(v2) > 0
      case LE => v1.compareTo(v2) <= 0
      case GE => v1.compareTo(v2) >= 0
      case CONTAINS => v1.toString contains v2.toString
      case STARTS_WITH => v1.toString startsWith v2.toString
      case ENDS_WITH => v1.toString endsWith v2.toString
      case MATCHES =>
        try v1.toString matches v2.toString
        catch { case ex: PatternSyntaxException => false }
      case LIKE => isLike(v1.toString, v2.toString)
      case _ => false
    }
  }

  def getOperator = operator
}

