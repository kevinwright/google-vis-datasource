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
package engine

import datatable.value.{NumberValue, Value, ValueType}

/**
 * Aggregates a set of values. Adds one value at a time to the aggregated set.
 * This allows getting the values of: minimum, maximum, sum, count and average for the aggregated
 * set. Each one of these values is available only where appropriate (for instance, you cannot
 * average on text values).
 * The set of values itself is not stored.
 * Only non-null values are considered for aggregation.
 *
 * @author Yoav G.
 */
class ValueAggregator(val valueType: ValueType) {
  /**
   * The maximum value found so far.
   */
  private var max = Value getNullValueFromValueType valueType
  /**
   * The minimum value found so far.
   */
  private var min = max
  /**
   * The sum of all aggregated values. Updated only for NumberValue.
   */
  private var sum: Double = 0
  /**
   * The number of non null values aggregated.
   */
  private var count: Int = 0

  /**
   * Aggregates an additional value. If this value is not null it is counted,
   * summed, and compared against the current maximum and minimum values to
   * consider replacing them.
   *
   * @param value The value to aggregate.
   */
  def aggregate(value: Value): Unit = {
    if (!value.isNull) {
      count += 1
      if (valueType == ValueType.NUMBER) {
        sum += (value.asInstanceOf[NumberValue]).getValue
      }
      if (count == 1) {
        max = value
        min = value
      } else {
        max = if(max.compareTo(value) >= 0) max else value
        min = if(min.compareTo(value) <= 0) min else value
      }
    } else if (count == 0) {
      min = value
      max = value
    }
  }

  /**
   * Returns the sum of all (non null) aggregated values.
   *
   * @return The sum of all (non null) aggregated values.
   *
   * @throws UnsupportedOperationException In case the column type does not
   *     support sum.
   */
  private def getSum: Double = {
    if (valueType != ValueType.NUMBER) throw new UnsupportedOperationException
    else sum
  }

  /**
   * Returns the average (or null if no non-null values were aggregated).
   *
   * @return The average (or null if no non-null values were aggregated).
   *
   * @throws UnsupportedOperationException If the column type does not support average.
   */
  private def getAverage: Option[Double] = {
    if (valueType != ValueType.NUMBER) throw new UnsupportedOperationException
    else if (count > 0) Some(sum / count)
    else None
  }

  /**
   * Returns a single value.
   * Note: The aggregation of a zero number of rows returns a null value for
   * all aggregation types except from count. The type of Null value is numeric
   * for sum and average and identical to its column values for min and max.
   *
   * @param type The type of aggregation requested.
   *
   * @return The requested value.
   */
  def getValue(aggType: AggregationType): Value = {
    import AggregationType._
    aggType match {
      case AVG =>
        if ((count != 0)) NumberValue(getAverage)
        else NumberValue.getNullValue
      case COUNT =>
        new NumberValue(count)
      case MAX =>
        if (count == 0) (Value getNullValueFromValueType max.getType)
        else max
      case MIN =>
        if (count == 0) (Value getNullValueFromValueType min.getType)
        else min
      case SUM =>
        if (count != 0) NumberValue(getSum)
        else NumberValue.getNullValue
      case _ =>
        throw new RuntimeException("Invalid AggregationType")
    }
  }

}

