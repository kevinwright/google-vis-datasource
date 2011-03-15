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
package base

import com.ibm.icu.text.UFormat
import java.text.FieldPosition
import java.text.ParseException
import java.text.ParsePosition

object BooleanFormat {
  def split(pattern: String) = {
    val valuePatterns = pattern.split(":")
    if (valuePatterns.length != 2) {
      throw new IllegalArgumentException("Cannot construct a boolean format " + "from " + pattern + ". The pattern must contain a single ':' " + "character")
    }
    new Object{
      val t = valuePatterns(0)
      val f = valuePatterns(1)
    }
  }
}

import BooleanFormat.split

/**
 * A UFormat that performs formatting and parsing for boolean values.
 *
 * The string representation of boolean values is determined by two strings passed
 * to the constructor of this class; a string for <code>TRUE</code> and a string for
 * <code>FALSE</code>.
 *
 * Examples for text strings representing a BooleanValue that can be
 * used in construction are:
 * 1) true, false
 * 2) TRUE, FALSE
 * 2) t, f
 * 3) yes, no
 * 4) YES, NO
 * 5) check, uncheck
 * 6) Green, Red
 *
 * @author Hillel M.
 */
class BooleanFormat(trueString: String, falseString: String) extends UFormat {

  if (trueString == null || falseString == null) {
    throw new NullPointerException
  }

  /**
   * Creates a BooleanFormat with default true/false formatting.
   */
  def this() = this("true", "false")

  /**
   * Constructs a boolean format from a pattern. The pattern must contain two
   * strings separated by colon, for example: "true:false".
   *
   * @param pattern The pattern from which to construct.
   */
  def this(pattern: String) = {
    this(split(pattern).t, split(pattern).f)
  }

  /**
   * Formats a Boolean and appends the result to a StringBuffer.
   *
   * @param obj The object to format.
   * @param appendTo The StringBuffer to which the formatted string will be appended.
   * @param pos A FieldPosition param (not used in this class).
   *
   * @return A StringBuffer with the formatted string for this object.
   */
  def format(obj: AnyRef, appendTo: StringBuffer, pos: FieldPosition): StringBuffer = {
    obj match {
      case null =>
        pos setBeginIndex 0
        pos setEndIndex 0
      case x: java.lang.Boolean if x == true =>
        appendTo append trueString
        pos setBeginIndex 0
        pos setEndIndex (trueString.length - 1)
      case x: java.lang.Boolean if x == false =>
        appendTo append falseString
        pos setBeginIndex 0
        pos setEndIndex (falseString.length - 1)
      case _ =>
        throw new IllegalArgumentException
    }
    appendTo
  }

  /**
   * Parses a string into a {@code Boolean}. A string can be either a trueString
   * or a falseString (non-case sensitive).
   *
   * @param source The string from which to parse.
   * @param pos Marks the end of the parsing, or 0 if the parsing failed.
   *
   * @return A {@code Boolean} for the parsed string.
   */
  def parseObject(source: String, pos: ParsePosition): java.lang.Boolean = {
    if (source == null) {
      throw new NullPointerException
    }
    var value =
      if (trueString.equalsIgnoreCase(source.trim)) {
        pos.setIndex(trueString.length)
        Some(true: java.lang.Boolean)
      } else if (falseString.equalsIgnoreCase(source.trim)) {
        pos.setIndex(falseString.length)
        Some(false: java.lang.Boolean)
      } else {
        None
      }
    if (value.isEmpty) pos.setErrorIndex(0)
    value.orNull
  }

  /**
   * Parses text from the beginning of the given string to produce a boolean.
   * The method may not use the entire text of the given string.
   *
   * @param text A String that should be parsed from it's start.
   *
   * @return A {@code Boolean} parsed from the string.
   *
   * @exception ParseException If the string cannot be parsed.
   */
  def parse(text: String): Boolean = {
    val parsePosition = new ParsePosition(0)
    val result = Boolean.unbox(parseObject(text, parsePosition))
    if (parsePosition.getIndex == 0) {
      throw new ParseException("Unparseable boolean: \"" + text + '"', parsePosition.getErrorIndex)
    }
    result
  }
}

