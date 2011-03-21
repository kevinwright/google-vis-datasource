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
package parser

import base.InvalidQueryException
import java.lang.IllegalArgumentException

import datatable.value.{DateTimeValue, DateValue, TimeOfDayValue}
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

/**
 * A utility class for the QueryParser. The functions here are called from the .jj file.
 *
 * Note on errors: Since this class handles a user generated query, all errors detected cause both
 * logging, and an exception that is thrown to the user.
 *
 * @author Yonatan B.Y.
 */
final object ParserUtils {
  /**
   * Log.
   */
  private val log = LogFactory getLog this.getClass
  /**
   * The message that should be in the exception thrown when parsing an invalid
   * date string, parameterized by the erroneous string itself.
   */
  private val dateMessage =
    "Invalid date literal [%1$s]. Date literals should be of form yyyy-MM-dd."

  private def dateError(s: String): Nothing = {
    val msg = dateMessage format s
    log error msg
    throw new InvalidQueryException(msg)
  }

  /**
   * The message that should be in the exception thrown when parsing an invalid
   * timeofday string, parameterized by the erroneous string itself.
   */
  private val timeOfDayMessage =
    "Invalid timeofday literal [%1$s]. Timeofday literals should be of form HH:mm:ss[.SSS]"

  private def timeOfDayError(s: String): Nothing = {
    val msg = timeOfDayMessage format s
    log error msg
    throw new InvalidQueryException(msg)
  }

  /**
   * The message that should be in the exception thrown when parsing an invalid
   * datetime string, parameterized by the erroneous string itself.
   */
  private val dateTimeMessage =
    "Invalid datetime literal [%1$s]. Datetime literals should be of form yyyy-MM-dd HH:mm:ss[.SSS]"

  private def dateTimeError(s: String): Nothing = {
    val msg = dateTimeMessage format s
    log error msg
    throw new InvalidQueryException(msg)
  }

  /**
   * Parses a string into a date value, for the query parser. The dates parsed are always in the
   * format: yyyy-MM-dd.
   *
   * @param s The string to parse.
   *
   * @return The parsed date.
   *
   * @throws InvalidQueryException If the string is not parse-able as a date of format yyyy-MM-dd.
   */
  def stringToDate(s: String): DateValue = {
    val split = s split '-'

    if (split.length != 3) dateError(s)
    try {
      val year = split(0).toInt
      val month = split(1).toInt - 1
      val day = split(2).toInt
      DateValue(year, month, day)
    } catch { case _:NumberFormatException | _:IllegalArgumentException => dateError(s) }
  }

  /**
   * Parses a string into a time-of-day value, for the query parser. The values parsed are always
   * in the format: HH:mm:ss[.SSS].
   *
   * @param s The string to parse.
   *
   * @return The parsed time-of-day.
   *
   * @throws InvalidQueryException If the string can not be parsed into a time-of-day of format
   *     HH:mm:ss[.SSS].
   */
  def stringToTimeOfDay(s: String): TimeOfDayValue = {
    val split = s split ':'
    if (split.length != 3) timeOfDayError(s)
    try {
      val hour = split(0).toInt
      val minute = split(1).toInt
      if (split(2) contains '.') {
        val secondMilliSplit = split(2) split '.'
        if (secondMilliSplit.length != 2) timeOfDayError(s)
        val second = secondMilliSplit(0).toInt
        var milli = secondMilliSplit(1).toInt
        TimeOfDayValue(hour, minute, second, milli)
      } else {
        val second = split(2).toInt
        TimeOfDayValue(hour, minute, second)
      }
    } catch { case _:NumberFormatException | _:IllegalArgumentException => timeOfDayError(s) }
  }

  /**
   * Parses a string into a date-time value, for the query parser. The values parsed are always
   * in the format: yyyy-MM-dd HH:mm:ss[.SSS].
   *
   * @param s The string to parse.
   *
   * @return The parsed date-time
   *
   * @throws InvalidQueryException If the string can not be parsed into a date-time of format
   *     yyyy-MM-dd HH:mm:ss[.SSS].
   */
  def stringToDatetime(s: String): DateTimeValue = {
    val mainSplit = s split ' '
    if (mainSplit.length != 2) dateTimeError(s)
    val dateSplit = mainSplit(0) split '-'
    val timeSplit = mainSplit(1) split ':'
    if (dateSplit.length != 3 || timeSplit.length != 3) dateTimeError(s)
    try {
      val year = dateSplit(0).toInt
      val month = dateSplit(1).toInt - 1
      val day = dateSplit(2).toInt
      val hour = timeSplit(0).toInt
      val minute = timeSplit(1).toInt
      if (timeSplit(2) contains '.') {
        val secondMilliSplit = timeSplit(2) split '.'
        if (secondMilliSplit.length != 2) dateTimeError(s)
        val second = secondMilliSplit(0).toInt
        val milli = secondMilliSplit(1).toInt
        DateTimeValue(year, month, day, hour, minute, second, milli)
      } else {
        val second = timeSplit(2).toInt
        DateTimeValue(year, month, day, hour, minute, second, 0)
      }
    } catch { case _:NumberFormatException | _:IllegalArgumentException => dateTimeError(s) }
  }

  /**
   * Strips the first and last characters from a string.
   * Throws a runtime exception if the string is less than 2 characters long. Used for stripping
   * quotes (whether single, double, or back-quotes) from, for example, "foo", 'bar', and `baz`.
   *
   * @param s The string from which to strip the quotes.
   *
   * @return The stripped string.
   */
  def stripQuotes(s: String): String = {
    if (s.length < 2) {
      throw new RuntimeException("String is of length < 2 on call to stripQuotes: " + s)
    }
    s.substring(1, s.length - 1)
  }

}


