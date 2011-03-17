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
package datatable
package value

import com.ibm.icu.util.{Calendar, GregorianCalendar, TimeZone}

/**
 * A value of type date-time. Used to represent a specific day in a given year as well as a
 * specific time during that day. This differs from {@link DateValue}, which represents only a
 * specific day in a given year.
 * DateTime is represented internally by a GregorianCalendar. The
 * calendar is immutable and is kept only for validation of the input.
 *
 * Default constructor creates a new instance based on the given
 * {@code GregorianCalendar}.
 * The given calendar's time zone must be set to "GMT" as a precondition to
 * use this constructor.
 * Note: The date time values: year, month, dayOfMonth, hour, minute, second
 * and millisecond correspond to the values returned by calendar.get(field)
 * of the given calendar.
 *
 * @author Hillel M.
 */
case class DateTimeValue(calendar: Option[GregorianCalendar]) extends Value {

  calendar foreach { cal =>
    if (cal.getTimeZone != (TimeZone getTimeZone "GMT")) {
      throw new IllegalArgumentException(
        "Can't create DateTimeValue from GregorianCalendar that is not GMT.")
    }
  }

  def this(cal: GregorianCalendar) = this(Option(cal.clone.asInstanceOf[GregorianCalendar]))


  /**
   *
   * @param calendar A Gregorian calendar on which to base this instance.
   *
   * @throws IllegalArgumentException When calendar time zone is not set
   *     to GMT.
   */

  def throwNullException = throw new NullValueException("This object is null")

  def getYear =
    calendar map (_ get Calendar.YEAR) getOrElse throwNullException

  def getMonth =
    calendar map (_ get Calendar.MONTH) getOrElse throwNullException

  def getDayOfMonth =
    calendar map (_ get Calendar.DAY_OF_MONTH) getOrElse throwNullException

  def getHourOfDay =
    calendar map (_ get Calendar.HOUR_OF_DAY) getOrElse throwNullException

  def getMinute =
    calendar map (_ get Calendar.MINUTE) getOrElse throwNullException

  def getSecond =
    calendar map (_ get Calendar.SECOND) getOrElse throwNullException

  def getMillisecond =
    calendar map (_ get Calendar.MILLISECOND) getOrElse throwNullException

  def getType = ValueType.DATETIME

  /**
   * Returns the DateTimeValue as a String using default formatting.
   *
   * @return The DateTimeValue as a String using default formatting.
   */
  override def toString = {
    calendar map { calendar =>
      val ms =if (getMillisecond > 0) {"." + "%1$03d".format(getMillisecond)} else ""
      "%1$d-%2$02d-%3$02d %4$02d:%5$02d:%6$02d%s".format(
        getYear,
        getMonth + 1,
        getDayOfMonth,
        getHourOfDay,
        getMinute,
        getSecond,
        ms)
    } getOrElse "null"
  }

  /**
   * Tests whether this value is a logical null.
   *
   * @return Indication if the value is null.
   */
  def isNull = calendar.isEmpty

  /**
   * Compares this value to another value of the same type.
   *
   * @param other Other value.
   *
   * @return 0 if equal, negative if this is smaller, positive if larger.
   */
  def compareTo(other: Value): Int = {
    if (this eq other) {
      return 0
    }
    var otherDateTime = other.asInstanceOf[DateTimeValue]
    if (isNull) {
      return -1
    }
    if (otherDateTime.isNull) {
      return 1
    }
    calendar map (_ compareTo otherDateTime.getCalendar) getOrElse 0
  }

  /**
   * Do not use GregorianCalendar
   * hash code since it does not consider time values and it might trigger
   * internal changes in the calendar values.
   * The hashCode of NULL_VALUE is zero.
   */
  override def hashCode: Int = {
    calendar map { calendar =>
      var hash: Int = 1579
      hash = (hash * 11) + getYear
      hash = (hash * 11) + getMonth
      hash = (hash * 11) + getDayOfMonth
      hash = (hash * 11) + getHourOfDay
      hash = (hash * 11) + getMinute
      hash = (hash * 11) + getSecond
      hash = (hash * 11) + getMillisecond
      hash
    } getOrElse 0
  }

  def getObjectToFormat: GregorianCalendar = calendar orNull

  /**
   * Returns the internal GregorianCalendar.
   *
   * @return The internal GregorianCalendar.
   *
   * @throws NullValueException Thrown when this Value is NULL_VALUE.
   */
  def getCalendar = calendar getOrElse throwNullException

  /**
   * {@inheritDoc}
   */
  protected def innerToQueryString: String = {
    val milli = Some(getMillisecond) filter (0!=) map {"." + _} getOrElse ""
    "DATETIME '" +
      getYear + "-" + (getMonth + 1) + "-" + getDayOfMonth +
      " " + getHourOfDay + ":" + getMinute + ":" +
      getSecond + milli + "'"
  }

}

object DateTimeValue {
  private final val NULL_VALUE = DateTimeValue(None)
  def getNullValue = NULL_VALUE

  /**
   * Creates a new DateTime value.
   * The input is checked using a gregorian calendar.
   * Note this uses the java convention for months:
   * January = 0, ..., December = 11.
   *
   * @param year The year.
   * @param month The month.
   * @param dayOfMonth The day of month.
   * @param hours The hours.
   * @param minutes The minutes.
   * @param seconds The seconds.
   * @param milliseconds The milliseconds.
   *
   * @throws IllegalArgumentException Thrown if one of the
   *     parameters is illegal.
   */
  def apply(
    year: Int,
    month: Int,
    dayOfMonth: Int,
    hours: Int,
    minutes: Int,
    seconds: Int,
    milliseconds: Int): DateTimeValue =
  {
    val calendar = new GregorianCalendar(
      year, month, dayOfMonth, hours, minutes, seconds)
    calendar.set(Calendar.MILLISECOND, milliseconds)
    calendar setTimeZone (TimeZone getTimeZone "GMT")
    if (
      (calendar get Calendar.YEAR) != year ||
      (calendar get Calendar.MONTH) != month ||
      (calendar get Calendar.DAY_OF_MONTH) != dayOfMonth ||
      (calendar get Calendar.HOUR_OF_DAY) != hours ||
      (calendar get Calendar.MINUTE) != minutes ||
      (calendar get Calendar.SECOND) != seconds ||
      (calendar get Calendar.MILLISECOND) != milliseconds)
    {
      throw new IllegalArgumentException("Invalid java date " + "(yyyy-MM-dd hh:mm:ss.S): " + year + '-' + month + '-' + dayOfMonth + ' ' + hours + ':' + minutes + ':' + seconds + '.' + milliseconds)
    }
    DateTimeValue(Some(calendar))
  }
}

