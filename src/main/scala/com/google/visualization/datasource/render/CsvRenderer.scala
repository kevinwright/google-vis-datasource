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
package render

import base.ResponseStatus
import datatable.ColumnDescription
import datatable.DataTable
import datatable.TableCell
import datatable.TableRow
import datatable.ValueFormatter
import datatable.value.ValueType
import com.ibm.icu.util.ULocale
import org.apache.commons.lang.StringUtils
import java.util.{List,Map}

import collection.JavaConverters._
/**
 * Takes a data table and returns a csv string.
 *
 * @author Nimrod T.
 */
object CsvRenderer {
  /**
   * Generates a csv string representation of a data table.
   *
   * @param dataTable The data table.
   * @param locale The locale. If null, uses the default from
   * {@code LocaleUtil#getDefaultLocale}.
   * @param separator The separator string used to delimit row values. 
   *     If the separator is {@code null}, comma is used as a separator.
   *
   * @return The char sequence with the csv string.
   */
  def renderDataTable(dataTable: DataTable, locale: ULocale, separatorOrNull: String): CharSequence = {
    val separator = if (separatorOrNull == null) "," else separatorOrNull
    if (dataTable.getColumnDescriptions.isEmpty) {
      return ""
    }
    val sb = new StringBuilder
    val columns = dataTable.getColumnDescriptions
    for (column <- columns.asScala) {
      sb append (escapeString(column.getLabel)) append separator
    }
    val formatters = ValueFormatter.createDefaultFormatters(locale)
    var length = sb.length
    sb.replace(length - 1, length, "\n")
    var rows = dataTable.getRows
    for (row <- rows.asScala) {
      for (cell <- row.getCells.asScala) {
        val formattedValueOrNull = cell.getFormattedValue
        val formattedValue = if (formattedValueOrNull == null) {
          formatters get cell.getType format cell.getValue
        } else formattedValueOrNull

        if (cell.isNull) {
          sb.append("null")
        } else {
          val valType = cell.getType
          if (formattedValue.indexOf(',') > -1 || valType == ValueType.TEXT) {
            sb append escapeString(formattedValue)
          } else {
            sb append formattedValue
          }
        }
        sb append separator
      }
      length = sb.length
      sb.replace(length - 1, length, "\n")
    }
    sb.toString
  }

  /**
   * Escapes a string that is written to a csv file. The escaping is as follows:
   * 1) surround with ".
   * 2) double each internal ".
   *
   * @param input The input string.
   *
   * @return An escaped string.
   */
  private def escapeString(input: String): String = {
    val sb = new StringBuilder
    sb append "\""
    sb append StringUtils.replace(input, "\"", "\"\"")
    sb append "\""
    sb.toString
  }

  /**
   * Renders an error message.
   *
   * @param responseStatus The response status.
   *
   * @return An error message.
   */
  def renderCsvError(responseStatus: ResponseStatus): String = {
    val sb = new StringBuilder
    sb append "Error: " append responseStatus.getReasonType.getMessageForReasonType(null)
    sb append ". " append responseStatus.getDescription
    escapeString(sb.toString)
  }
}

