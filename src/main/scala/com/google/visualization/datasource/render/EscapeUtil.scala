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
package com.google.visualization.datasource.render

import org.apache.commons.lang.StringEscapeUtils

/**
 * A utility to escape strings.
 *
 * @author Hillel M.
 */
object EscapeUtil {
  /**
   * This method is used to escape strings embedded in the json response. The method is based on
   * org.apache.shindig.common.JsonSerializer.appendString().
   * The method escapes the following in order to enable safe parsing of the json string:
   * 1) single and double quotes - ' and "
   * 2) backslash - /
   * 3) html brackets - <>
   * 4) control characters - \n \t \r ..
   * 5) special characters - out of range unicode characters (formatted to the uxxxx format)
   *
   * @param str The original string to escape.
   *
   * @return The escaped string.
   */
  def jsonEscape(str: String): String = {
    def needsUnicodeEscaping(char: Char) =
      char < ' ' || (char >= '\u0080' && char < '\u00a0') || (char >= '\u2000' && char < '\u2100')

    if (str == null || str.length == 0) {
      return ""
    }
    var sb = new StringBuffer(str.length + 10)
    str foreach { char =>
      val out = char match {
        case '\'' => "\\u0027"
        case '\"' => "\\u0022"
        case '\\' => "\\" + char
        case '<' => "\\u003c"
        case '>' => "\\u003e"
        case '\b' => "\\b"
        case '\t' => "\\t"
        case '\n' => "\\n"
        case '\f' => "\\f"
        case '\r' => "\\r"
        case _ if needsUnicodeEscaping(char) =>
          "\\u" +
            HEX_DIGITS((char >> 12) & 0xF) +
            HEX_DIGITS((char >> 8) & 0xF) +
            HEX_DIGITS((char >> 4) & 0xF) +
            HEX_DIGITS(char & 0xF)
        case _ => char
      }
      sb append out
    }
    sb.toString
  }

  /**
   * Escapes the string for embedding in html.
   *
   * @param str The string to escape.
   *
   * @return The escaped string.
   */
  def htmlEscape(str: String) = StringEscapeUtils.escapeHtml(str)

  /**
   * This helper lookup array is used for json escaping. It enables fast lookup of unicode
   * characters.
   * {@link #jsonEscape}.
   */
  val HEX_DIGITS: Array[Char] = "0123456789ABCDEF".toArray
}
