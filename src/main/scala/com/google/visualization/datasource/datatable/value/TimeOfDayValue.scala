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
 * A value of type time-of-day.
 * Time is represented internally by four fields: hours, minutes, seconds and milliseconds.
 *
 * Default constructor creates a new instance based on the given
 * {@code GregorianCalendar}.
 * The given calendar's time zone must be set to "GMT" as a precondition to
 * use this constructor.
 * Note: The date time values: hour, minute, second and millisecond
 * correspond to the values returned by calendar.get(field) of the given
 * calendar.
 *
 * @author Hillel M.
 */

case class TimeOfDayValue(calendar: Option[GregorianCalendar]) extends Value {

  calendar foreach { cal =>
    if (cal.getTimeZone != (TimeZone getTimeZone "GMT")) {
      throw new IllegalArgumentException(
        "Can't create DateTimeValue from GregorianCalendar that is not GMT.")
    }
  }

  def this(cal: GregorianCalendar) = this(Option(cal.clone.asInstanceOf[GregorianCalendar]))

  def throwNull = throw new NullValueException("This object is null")

  lazy val hours = calendar map (_ get Calendar.HOUR_OF_DAY) getOrElse throwNull
  lazy val minutes = calendar map (_ get Calendar.MINUTE) getOrElse throwNull
  lazy val seconds = calendar map (_ get Calendar.SECOND) getOrElse throwNull
  lazy val milliseconds = calendar map (_ get Calendar.MILLISECOND) getOrElse throwNull

  def getType = ValueType.TIMEOFDAY


  override def toString: String = {
    calendar map { cal =>
      val ms = Some(milliseconds) filter (0!=) map ("." + "%1$3d".format(_)) getOrElse ""
      "%1$02d:%2$02d:%3$02d%s".format(hours, minutes, seconds, ms)
    } getOrElse "null"
  }

  def isNull = calendar.isEmpty

  def compareTo(other: Value): Int = {
    if (this eq other) {
      return 0
    }
    var otherTimeOfDay: TimeOfDayValue = other.asInstanceOf[TimeOfDayValue]
    if (isNull) {
      return -1
    }
    if (otherTimeOfDay.isNull) {
      return 1
    }
    if (this.hours > otherTimeOfDay.hours) {
      return 1
    }
    else if (this.hours < otherTimeOfDay.hours) {
      return -1
    }
    if (this.minutes > otherTimeOfDay.minutes) {
      return 1
    }
    else if (this.minutes < otherTimeOfDay.minutes) {
      return -1
    }
    if (this.seconds > otherTimeOfDay.seconds) {
      return 1
    }
    else if (this.seconds < otherTimeOfDay.seconds) {
      return -1
    }
    if (this.milliseconds > otherTimeOfDay.milliseconds) {
      return 1
    }
    else if (this.milliseconds < otherTimeOfDay.milliseconds) {
      return -1
    }
    return 0
  }

  override lazy val hashCode: Int = {
    calendar map { cal =>
      var hash: Int = 1193
      hash = (hash * 13) + hours
      hash = (hash * 13) + minutes
      hash = (hash * 13) + seconds
      hash = (hash * 13) + milliseconds
      hash
    } getOrElse 0
  }

  /**
   * A method to retrieve a formattable object for this object.
   * It is important to set the GMT TimeZone to avoid conversions related to TimeZone.
   */
  override def getObjectToFormat: Calendar = {
    calendar map { c =>
      var cal: Calendar = new GregorianCalendar(TimeZone getTimeZone "GMT")
      cal.set(Calendar.YEAR, 1899)
      cal.set(Calendar.MONTH, Calendar.DECEMBER)
      cal.set(Calendar.DAY_OF_MONTH, 30)
      cal.set(Calendar.HOUR_OF_DAY, hours)
      cal.set(Calendar.MINUTE, minutes)
      cal.set(Calendar.SECOND, seconds)
      cal.set(Calendar.MILLISECOND, milliseconds)
      cal
    } orNull
  }


  def getHours = hours
  def getMinutes = minutes
  def getSeconds = seconds
  def getMilliseconds = milliseconds

  def innerToQueryString: String = {
    val ms = Some(milliseconds) filter (0!=) map ("." + _) getOrElse  ""
    "TIMEOFDAY '" + hours + ":" + minutes + ":" + seconds + ms + "'"
  }
}

object TimeOfDayValue {
  private val NULL_VALUE = new TimeOfDayValue(None)
  def getNullValue = NULL_VALUE

  /**
   * Creates a new TimeOfDayValue.
   *
   * @param hours The hours.
   * @param minutes The minutes.
   * @param seconds The seconds.
   *
   * @throws IllegalArgumentException Thrown when one of the
   * paramters is illegal.
   */
  def apply(hours: Int, minutes: Int, seconds: Int): TimeOfDayValue =
    apply(hours, minutes, seconds, 0)

  /**
   * Creates a new TimeOfDayValue.
   *
   * @param hours The hours.
   * @param minutes The minutes.
   * @param seconds The seconds.
   * @param milliseconds The milliseconds.
   *
   * @throws IllegalArgumentException Thrown when one of the
   *     parameters is illegal.
   */
  def apply(hours: Int, minutes: Int, seconds: Int, milliseconds: Int): TimeOfDayValue = {
    if ((hours >= 24) || (hours < 0)) {
      throw new IllegalArgumentException("This hours value is invalid: " + hours)
    }
    if ((minutes >= 60) || (minutes < 0)) {
      throw new IllegalArgumentException("This minutes value is invalid: " + minutes)
    }
    if ((seconds >= 60) || (seconds < 0)) {
      throw new IllegalArgumentException("This seconds value is invalid: " + seconds)
    }
    if ((milliseconds >= 1000) || (milliseconds < 0)) {
      throw new IllegalArgumentException("This milliseconds value is invalid: " + milliseconds)
    }

    val calendar = new GregorianCalendar(
      1899, Calendar.DECEMBER, 30,
      hours, minutes, seconds)
      calendar.set(Calendar.MILLISECOND, milliseconds)

    calendar setTimeZone (TimeZone getTimeZone "GMT")
    TimeOfDayValue(Some(calendar))
  }

}
