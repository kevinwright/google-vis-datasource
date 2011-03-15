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

import com.google.common.collect.Maps

import base.{BooleanFormat, LocaleUtil, TextFormat}
import value._

import com.ibm.icu.text.{DecimalFormat, DecimalFormatSymbols, NumberFormat, SimpleDateFormat, UFormat}
import com.ibm.icu.util.{GregorianCalendar, TimeZone, ULocale}
import java.text.ParseException
import java.util.{Date,Map}

/**
 * Formats a {@link Value}, or parses a string to create a {@link Value}.
 * An instance of this class can be created using the
 * {@link #createFromPattern(ValueType, String, ULocale)} method, and can then use the format
 * and/or parse.
 *
 * The class also supplies a set of default patterns per {@link ValueType}. The default patterns
 * can be used for parsing/formatting values when there is no specified pattern.
 * Otherwise, create a class instance by specifying a pattern and locale.
 *
 * Note: This class is not thread safe since it uses {@code UFormat}. 
 *
 * @author Yonatan B.Y.
 */
object ValueFormatter {
  /**
   * Creates a formatter for the given value type with the given pattern string and locale.
   * If the pattern is illegal returns null.
   * If pattern is null, uses the first default pattern for the given type.
   * If ulocale is null, uses the default locale return by {@code LocaleUtil#getDefaultLocale}.
   *
   * @param type The column value type.
   * @param pattern The string pattern representing the formatter pattern.
   * @param locale The ULocale of the formatter.
   *
   * @return A formatter for the given type, pattern and locale, or null if the pattern is illegal.
   */
  def createFromPattern(valType: ValueType, patternOrNull: String, localeOrNull: ULocale): ValueFormatter = {
    val pattern =
      if (patternOrNull == null) { getDefaultPatternByType(valType) }
      else patternOrNull

    val locale =
      if (localeOrNull == null) { LocaleUtil.getDefaultLocale }
      else localeOrNull

    val uFormat = try {
      valType match {
        case ValueType.BOOLEAN =>
          val uFormat = new BooleanFormat(pattern)
          uFormat format BooleanValue.TRUE.getObjectToFormat
          uFormat
        case ValueType.TEXT =>
          new TextFormat
        case ValueType.DATE =>
          val uFormat = new SimpleDateFormat(pattern, locale)
          uFormat.asInstanceOf[SimpleDateFormat] setTimeZone (TimeZone getTimeZone "GMT")
          uFormat.format(DateValue(1995, 7, 3).getObjectToFormat)
          uFormat
        case ValueType.TIMEOFDAY =>
          val uFormat = new SimpleDateFormat(pattern, locale)
          uFormat.asInstanceOf[SimpleDateFormat] setTimeZone (TimeZone getTimeZone "GMT")
          uFormat.format(TimeOfDayValue(2, 59, 12, 123).getObjectToFormat)
          uFormat
        case ValueType.DATETIME =>
          val uFormat = new SimpleDateFormat(pattern, locale)
          uFormat.asInstanceOf[SimpleDateFormat] setTimeZone (TimeZone getTimeZone "GMT")
          uFormat.format(DateTimeValue(1995, 7, 3, 2, 59, 12, 123).getObjectToFormat)
          uFormat
        case ValueType.NUMBER =>
          val symbols = new DecimalFormatSymbols(locale)
          val uFormat = new DecimalFormat(pattern, symbols)
          uFormat.format(new NumberValue(-12.3).getObjectToFormat)
          uFormat
        case _ => null
      }
    } catch { case e: RuntimeException => null }

    new ValueFormatter(pattern, uFormat, valType, locale)
  }

  /**
   * Creates a default formatter for the specified value type and locale.
   * If locale is null, uses the default locale returned by {@code LocaleUtil#getDefaultLocale}.
   *
   * @param type The value type.
   * @param locale The data table locale.
   *
   * @return A default formatter for the given type and locale.
   */
  def createDefault(valType: ValueType, locale: ULocale): ValueFormatter = {
    var pattern = getDefaultPatternByType(valType)
    createFromPattern(valType, pattern, locale)
  }

  /**
   * Creates default formatters for all the value types for the specified locale.
   * Returns a map of default formatters by type.
   * The map can be used for iterating over a data table and parsing/formatting its values.
   *
   * @param locale The data table locale.
   *
   * @return A map of default formatters by type with the given locale.
   */
  def createDefaultFormatters(locale: ULocale): Map[ValueType, ValueFormatter] = {
    var foramtters: Map[ValueType, ValueFormatter] = Maps.newHashMap[ValueType, ValueFormatter]
    for (`type` <- ValueType.values) {
      foramtters.put(`type`, createDefault(`type`, locale))
    }
    return foramtters
  }

  /**
   * Returns the default pattern for the specified value type and index.
   *
   * @param type The value type.
   *
   * @return The default pattern for the specified value type and index.
   */
  private def getDefaultPatternByType(valType: ValueType): String = valType match {
    case ValueType.TEXT => DEFAULT_TEXT_DUMMY_PATTERN
    case ValueType.DATE => DEFAULT_DATE_PATTERNS
    case ValueType.DATETIME => DEFAULT_DATETIME_PATTERN
    case ValueType.TIMEOFDAY => DEFAULT_TIMEOFDAY_PATTERN
    case ValueType.BOOLEAN => DEFAULT_BOOLEAN_PATTERN
    case ValueType.NUMBER => DEFAULT_NUMBER_PATTERN
    case _ => null
  }

  /**
   * The default pattern for parsing a string to a text value.
   *
   * @see com.google.visualization.datasource.datatable.value.TextValue
   */
  private final val DEFAULT_TEXT_DUMMY_PATTERN: String = "dummy"
  /**
   * The default pattern for parsing a string to a date time value.
   *
   * @see DateTimeValue
   */
  private final val DEFAULT_DATETIME_PATTERN: String = "yyyy-MM-dd HH:mm:ss"
  /**
   * The default pattern for parsing a string to a date value.
   *
   * @see DateValue
   */
  private final val DEFAULT_DATE_PATTERNS: String = "yyyy-MM-dd"
  /**
   * The default pattern for parsing a string to a time of day value.
   *
   * @see TimeOfDayValue
   */
  private final val DEFAULT_TIMEOFDAY_PATTERN: String = "HH:mm:ss"
  /**
   * The default pattern for parsing a string to a boolean value.
   *
   * @see BooleanValue
   */
  private final val DEFAULT_BOOLEAN_PATTERN: String = "true:false"
  /**
   * The default pattern for parsing a string to a number value.
   *
   * @see NumberValue
   */
  private final val DEFAULT_NUMBER_PATTERN: String = ""
}

class ValueFormatter private (pattern: String, uFormat: UFormat, valType: ValueType, locale: ULocale)  {


  /**
   * Formats a value to a string, using the given pattern.
   *
   * @param value The value to format.
   *
   * @return The formatted value.
   */
  def format(value: Value) =
    if (value.isNull) "" else  { uFormat format value.getObjectToFormat }

  /**
   * Creates the corresponding {@code Value} from the given string.
   * If parsing fails, returns a NULL_VALUE for the specified type.
   * For example, if val="3" and type=ValueType.Number, then after successfully parsing
   * the string "3" into the double 3.0 a new NumberValue would
   * be returned with an internal double value = 3.
   *
   * @param val The string to parse.
   *
   * @return A corresponding {@code Value} for the given string. If parsing fails the value would
   * be a NULL_VALUE of the correct {@code ValueType}.
   */
  def parse(str: String): Value = {
    try {
      valType match {
        case ValueType.DATE => parseDate(str)
        case ValueType.TIMEOFDAY => parseTimeOfDay(str)
        case ValueType.DATETIME => parseDateTime(str)
        case ValueType.NUMBER => parseNumber(str)
        case ValueType.BOOLEAN => parseBoolean(str)
        case ValueType.TEXT => new TextValue(str)
      }
    } catch {
      case pe: ParseException => Value.getNullValueFromValueType(valType)
    }
  }

  private def parseBoolean(str: String): BooleanValue = {
    val bool = uFormat.asInstanceOf[BooleanFormat] parse str
    BooleanValue getInstance bool
  }

  private def parseNumber(str: String): NumberValue = {
    val n = uFormat.asInstanceOf[NumberFormat] parse str
    new NumberValue(n.doubleValue)
  }

  private def parseDateTime(str: String): DateTimeValue = {
    val date = uFormat.asInstanceOf[SimpleDateFormat] parse str
    val cal = new GregorianCalendar(TimeZone getTimeZone "GMT")
    cal setTime date
    new DateTimeValue(cal)
  }

  private def parseDate(str: String): DateValue = {
    val date = uFormat.asInstanceOf[SimpleDateFormat] parse str
    val cal = new GregorianCalendar(TimeZone getTimeZone "GMT")
    cal setTime date
    new DateValue(cal)
  }

  private def parseTimeOfDay(str: String): TimeOfDayValue = {
    val date =  uFormat.asInstanceOf[SimpleDateFormat] parse str
    val cal = new GregorianCalendar(TimeZone getTimeZone "GMT")
    cal setTime date
    new TimeOfDayValue(cal)
  }

  def getUFormat = uFormat
  def getPattern = pattern
  def getLocale = locale
  def getType = valType

}

