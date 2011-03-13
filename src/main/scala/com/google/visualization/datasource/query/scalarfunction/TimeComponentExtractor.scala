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
package scalarfunction

import com.google.common.collect.Maps
import base.InvalidQueryException
import datatable.value.{DateTimeValue, DateValue, NumberValue, TimeOfDayValue, Value, ValueType}
import com.ibm.icu.util.{GregorianCalendar, Calendar}
import java.util.List

import collection.JavaConverters._

/**
 * A time component extractor. This class encompasses all the unary scalar functions
 * that extract a time component from a date/datetime/timeofday value. e.g.,
 * year, month, second.
 *
 * @author Liron L.
 */
case class TimeComponentExtractor private (val timeComponent: TimeComponentExtractorComponent) extends ScalarFunction {

  import TimeComponentExtractorComponent._

  def functionName = timeComponent.getName
  def getFunctionName = functionName

  /**
   * Executes the scalar function that extracts the timeComponent on the given
   * values. Returns a NumberValue with the timeComponent of the given
   * Date/DateTime/TimeOfDay value. The method does not validate the parameters,
   * the user must check the parameters before calling this method.
   *
   * @param values A list of the values on which the scalar function is performed.
   *
   * @return Value with the timeComponent value of the given value, or number
   *     null value if value is null.
   */
  override def evaluate(values: List[Value]): Value = {
    val value = values get 0
    val valueType = value.getType
    if (value.isNull) {
      NumberValue.getNullValue
    } else {
      val result = (timeComponent, value) match {
        case (YEAR, v: DateValue) => v.getYear
        case (YEAR, v: DateTimeValue) => v.getYear
        case (MONTH, v: DateValue) => v.getMonth
        case (MONTH, v: DateTimeValue) => v.getMonth
        case (DAY, v: DateValue) => v.getDayOfMonth
        case (DAY, v: DateTimeValue) => v.getDayOfMonth
        case (HOUR, v: TimeOfDayValue) => v.getHours
        case (HOUR, v: DateTimeValue) => v.getHourOfDay
        case (MINUTE, v: TimeOfDayValue) => v.getMinutes
        case (MINUTE, v: DateTimeValue) => v.getMinute
        case (SECOND, v: TimeOfDayValue) => v.getSeconds
        case (SECOND, v: DateTimeValue) => v.getSecond
        case (MILLISECOND, v: TimeOfDayValue) => v.getMilliseconds
        case (MILLISECOND, v: DateTimeValue) => v.getMillisecond
          //  `/ 3 + 1` to get 1-4 instead of 0-3
        case (QUARTER, v: DateValue) => v.getMonth / 3 + 1
        case (QUARTER, v: DateTimeValue) => v.getMonth / 3 + 1
        case (DAY_OF_WEEK, v: DateValue) =>
          val calendar = v.getObjectToFormat.asInstanceOf[GregorianCalendar]
          calendar get Calendar.DAY_OF_WEEK
        case (DAY_OF_WEEK, v: DateTimeValue) =>
          val calendar = v.getObjectToFormat.asInstanceOf[GregorianCalendar]
          calendar get Calendar.DAY_OF_WEEK
        case _ =>
          throw new RuntimeException("An invalid time component.")
      }
      new NumberValue(result)
    }
  }

  /**
   * Returns the return type of the function. In this case, NUMBER. The method
   * does not validate the parameters, the user must check the parameters
   * before calling this method.
   *
   * @param types A list of the types of the scalar function parameters.
   *
   * @return The type of the returned value: Number.
   */
  def getReturnType(types: List[ValueType]) = ValueType.NUMBER

  /**
   * Validates that there is only one parameter given for the function, and
   * that its type is DATE or DATETIME if the timeComponent to extract is year,
   * month or day, or that it is DATETIME or TIMEOFDAY if the timeComponent is
   * hour, minute, second or millisecond. Throws a ScalarFunctionException if
   * the parameters are invalid.
   *
   * @param types A list of parameter types.
   *
   * @throws InvalidQueryException Thrown if the parameters are invalid.
   */
  override def validateParameters(types: List[ValueType]): Unit = {
    if (types.size != 1) {
      throw new InvalidQueryException("Number of parameters for " + timeComponent.getName + "function is wrong: " + types.size)
    }
    timeComponent match {
      case YEAR | MONTH | DAY | QUARTER | DAY_OF_WEEK =>
        if ((types.get(0) != ValueType.DATE) && (types.get(0) != ValueType.DATETIME)) {
          throw new InvalidQueryException("Can't perform the function " + timeComponent.getName + " on a column that is not a Date or" + " a DateTime column")
        }
      case HOUR | MINUTE | SECOND | MILLISECOND =>
        if ((types.get(0) != ValueType.TIMEOFDAY) && (types.get(0) != ValueType.DATETIME)) {
          throw new InvalidQueryException("Can't perform the function " + timeComponent.getName + " on a column that is not a " + "TimeOfDay or a DateTime column")
        }
      case _ =>
    }
  }

  def toQueryString(argumentsQueryStrings: List[String]) =
    functionName + "(" + argumentsQueryStrings.get(0) + ")"

}

object TimeComponentExtractor {
  import TimeComponentExtractorComponent._

  /**
   * Returns an instance of a TimeComponentExtractor that extracts the given
   * TimeComponent.
   *
   * @param timeComponent The TimeComponent to extract.
   *
   * @return Returns an instance of a TimeComponentExtractor.
   */
  def getInstance(timeComponent: TimeComponentExtractorComponent) =
    instances(timeComponent)

  /**
   * A mapping of a TimeComponent to a TimeComponentExtractor that extracts this TimeComponent.
   * This is used to keep a single instance of every type of extractor.
   */
  private val instances = Map(
    YEAR -> TimeComponentExtractor(YEAR),
    MONTH -> TimeComponentExtractor(MONTH),
    WEEK -> TimeComponentExtractor(WEEK),
    DAY -> TimeComponentExtractor(DAY),
    HOUR -> TimeComponentExtractor(HOUR),
    MINUTE -> TimeComponentExtractor(MINUTE),
    SECOND -> TimeComponentExtractor(SECOND),
    MILLISECOND -> TimeComponentExtractor(MILLISECOND),
    QUARTER -> TimeComponentExtractor(QUARTER),
    DAY_OF_WEEK -> TimeComponentExtractor(DAY_OF_WEEK))
}
