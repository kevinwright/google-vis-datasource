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
package com.google.visualization.datasource.datatable.value

import com.ibm.icu.util.{Calendar, GregorianCalendar, TimeZone}

/**
 * A value of type date. Used to represent a specific day in a given year. This differs from
 * {@link DateTimeValue}, which represents a specific day in a given year as well as a specific
 * time during that day. 
 * Date is represented internally by three values: year, month and dayOfMonth.
 * This class stores only legitimate dates where the validation is done using
 * ibm.icu.GregorianCalendar.
 *
 * Default constructor creates a new instance based on the given
 * {@code GregorianCalendar}.
 * The given calendar's time zone must be set to "GMT" as a precondition to
 * use this constructor.
 *
 * @author Hillel M.
 */
case class DateValue(calendar: Option[Calendar]) extends Value {

  def this(cal: Calendar) = this(Option(cal))
  
  def throwNullException = throw new NullValueException("This object is null")

  calendar foreach { cal =>
    if (cal.getTimeZone != (TimeZone getTimeZone "GMT")) {
      throw new IllegalArgumentException(
        "Can't create DateValue from GregorianCalendar that is not GMT.")
    }
  }

  lazy val year =
    calendar map (_ get Calendar.YEAR) getOrElse throwNullException

  /**
   * Underlying value: month. Note we use the java convention for months, this is:
   * January = 0, February = 1, ..., December = 11.
   */
  lazy val month =
    calendar map (_ get Calendar.MONTH) getOrElse throwNullException

  /**
   * Underlying value: day of month.
   */
  lazy val dayOfMonth =
    calendar map (_ get Calendar.DAY_OF_MONTH) getOrElse throwNullException


  def getType = ValueType.DATE

  override def toString = calendar map { cal =>
    "%1$d-%2$02d-%3$02d".format(year, month + 1, dayOfMonth)
  } getOrElse "null"

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
    val otherDate: DateValue = other.asInstanceOf[DateValue]
    if (isNull) {
      return -1
    }
    if (otherDate.isNull) {
      return 1
    }
    if (this.year > otherDate.year) {
      return 1
    }
    else if (this.year < otherDate.year) {
      return -1
    }
    if (this.month > otherDate.month) {
      return 1
    }
    else if (this.month < otherDate.month) {
      return -1
    }
    if (this.dayOfMonth > otherDate.dayOfMonth) {
      return 1
    }
    else if (this.dayOfMonth < otherDate.dayOfMonth) {
      return -1
    }
    return 0
  }

  def getObjectToFormat: Calendar = calendar orNull

  def getYear: Int = year
  def getMonth: Int = month
  def getDayOfMonth: Int = dayOfMonth

  /**
   * {@inheritDoc}
   */
  protected def innerToQueryString: String = {
    "DATE '" + year + "-" + (month + 1) + "-" + dayOfMonth + "'"
  }
}


object DateValue {
  private val NULL_VALUE = DateValue(None)

  /**
   * Static method to return the null value (same one for all calls).
   *
   * @return Null value.
   */
  def getNullValue = NULL_VALUE

  /**
   * Creates a new date value.
   * The input is checked using a GregorianCalendar.
   * Note that we use java convention for months:
   * January = 0, ..., December = 11.
   *
   * @param year The year.
   * @param month The month.
   * @param dayOfMonth The day in the month.
   *
   * @throws IllegalArgumentException Thrown when one of the
   *     parameters is illegal.
   */
  def apply(year: Int, month: Int, dayOfMonth: Int): DateValue = {
    val calendar = new GregorianCalendar(year, month, dayOfMonth)

    if (((calendar get Calendar.YEAR) != year) ||
        ((calendar get Calendar.MONTH) != month) ||
        ((calendar get Calendar.DAY_OF_MONTH) != dayOfMonth)) {
      throw new IllegalArgumentException(
        "Invalid java date (yyyy-MM-dd): " + year + '-' + month + '-' + dayOfMonth)
    }
    else DateValue(Some(calendar))
  }


}
