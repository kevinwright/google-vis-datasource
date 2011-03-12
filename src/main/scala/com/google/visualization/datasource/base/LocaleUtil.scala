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

import com.ibm.icu.text.MessageFormat
import com.ibm.icu.util.ULocale
import java.util.{Locale, ResourceBundle}
import java.util.regex.{Matcher, Pattern}

/**
 * A utility class for locale handling.
 *
 * @author Yaniv S.
 *
 */
object LocaleUtil {
  /**
   * A regular expression for extracting the language, country and variant from a locale string.
   */
  private val LOCALE_PATTERN = Pattern.compile("(^[^_-]*)(?:[_-]([^_-]*)(?:[_-]([^_-]*))?)?")
  /**
   * The default locale. Used as a fall-back locale throughout the system.
   */
  private var defaultLocale = ULocale.US

  /**
   * Converts a locale string from the RFC 3066 standard format to the Java locale format.
   * You can call this on any locale string obtained from an external source
   * (cookie, URL parameter, header, etc.). This method accepts more than just the standard
   * format and will also tolerate capitalization discrepancies and the use of an underscore
   * in place of a hyphen.
   *
   * @param s The locale string.
   *
   * @return The locale for the given locale string.
   *
   */
  def getLocaleFromLocaleString(s: String): Locale = {
    Option(s) map { s =>
      val matcher = LOCALE_PATTERN.matcher(s)
      matcher.find
      val language = Option(matcher group 1) getOrElse ""
      var country = Option(matcher group 2) getOrElse ""
      var variant = Option(matcher group 3) getOrElse ""

      new Locale(language, country, variant)
    } orNull
  }

  /**
   * Sets the default locale.
   *
   * @param defaultLocale The default locale.
   */
  def setDefaultLocale(defaultLocale: ULocale): Unit =
    LocaleUtil.defaultLocale = defaultLocale

  /**
   * Returns the default locale.
   *
   * @return The default locale.
   */
  def getDefaultLocale = defaultLocale

  /**
   * Returns a localized message from the specified <code>ResourceBundle</code> for the given key.
   * In case the locale is null, uses the default locale.
   * If locale is null, the default <code>ResourceBundle</code> is used.
   *
   * @param bundleName The name of the resource bundle.
   * @param key The key of the requested string.
   * @param locale The locale.
   *
   * @return A localized message from the bundle based on the given locale.
   */
  def getLocalizedMessageFromBundle(bundleName: String, key: String, locale: Locale): String = {
    val bundle = locale match {
      case null => ResourceBundle.getBundle(bundleName)
      case x => ResourceBundle.getBundle(bundleName, x)
    }
    bundle getString key
  }

  /**
   * Returns a localized message from the specified <code>ResourceBundle</code> for the given key
   * with the given arguments inserted to the message in the specified locations.
   * In case the locale is null, uses the default locale.
   * If locale is null, the default <code>ResourceBundle</code> is used.
   *
   *
   * @param bundleName The name of the resource bundle.
   * @param key The key of the requested string.
   * @param args Arguments to place in the error message.
   * @param locale The locale.
   *
   * @return A localized message from the bundle based on the given locale.
   */
  def getLocalizedMessageFromBundleWithArguments(bundleName: String, key: String, args: Array[String], locale: Locale): String = {
    val rawMessage = getLocalizedMessageFromBundle(bundleName, key, locale)
    if (args != null && args.length > 0) {
      MessageFormat.format(rawMessage, args.asInstanceOf[Array[Object]])
    } else rawMessage
  }
}
