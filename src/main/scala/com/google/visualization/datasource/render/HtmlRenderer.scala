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

import com.google.visualization.datasource.base._
import com.google.visualization.datasource.datatable._
import com.google.visualization.datasource.datatable.value._
import com.ibm.icu.util.ULocale
import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import java.util.regex.Pattern

import collection.JavaConverters._

/**
 * Takes a data table and returns an html string.
 *
 * @author Nimrod T.
 */
object HtmlRenderer {
  def repeat[T](xs: T*) = Iterator.continually(xs.toIterator).flatten

  /**
   * Generates an HTML string representation of a data table.
   *
   * @param dataTable The data table to render.
   * @param locale The locale. If null, uses the default from
   * {@code LocaleUtil#getDefaultLocale}.
   *
   * @return The char sequence with the html string.
   */
  def renderDataTable(dataTable: DataTable, locale: ULocale): CharSequence = {
    val strBuf = new StringBuilder

    strBuf append header
    strBuf append "<table border='1' cellpadding='2' cellspacing='0'>"

    val columnDescriptions = dataTable.getColumnDescriptions.asScala

    val headerCells = columnDescriptions map { desc =>
      "<td>" + desc.getLabel + "</td>"
    }

    strBuf append headerCells.mkString(
      "<tr style='font-weight: bold; background-color: #aaa;'>",
      "\n",
      "</tr>")

    val formatters = (ValueFormatter createDefaultFormatters locale).asScala

    val bgColour = repeat("#f0f0f0", "#ffffff")

    dataTable.getRows.asScala foreach { row =>
      strBuf append renderTableRow(formatters, columnDescriptions, row, bgColour.next)
    }

    dataTable.getWarnings.asScala foreach { warning =>
      strBuf append "<br/><br/><div>"
      strBuf append (warning.getReasonType.getMessageForReasonType + ". " + warning.getMessage)
      strBuf append "</div>"
    }
    strBuf.toString
  }

  private def renderTableRow(formatters: ValueType => ValueFormatter, columnDescriptions: Seq[ColumnDescription], row: TableRow, bgColour: String) = {
    val strBuf = new StringBuilder

    strBuf append "<tr style='background-color:'%s'>".format(bgColour)

    (row.getCells.asScala zip columnDescriptions) map { case (cell,colDesc) =>
      val valueType = colDesc.getType

      val cellFormattedText = Option(cell.getFormattedValue) getOrElse {
        formatters(cell.getType) format cell.getValue
      }

      val (content, align) =
        if (cell.isNull)
          "\u00a0" -> None
        else {
          valueType match {
            case ValueType.NUMBER =>
              cellFormattedText ->
              Some("right")
            case ValueType.BOOLEAN =>
              val booleanValue = cell.getValue.asInstanceOf[BooleanValue]
              (if (booleanValue.getValue) "\u2714" else "\u2717") ->
              Some("center")
            case _ =>
              (if (StringUtils isEmpty cellFormattedText) "\u00a0" else cellFormattedText) ->
              None
          }
        }
      strBuf append "<td%s>%s</td>".format(
        align map { a => " align='%s'".format(a) } getOrElse "",
        content
      )
    }

    strBuf.toString
  }


//    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no")
//    transformer.setOutputProperty(OutputKeys.INDENT, "yes")
//    transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, "-//W3C//DTD HTML 4.01//EN")
//    transformer.setOutputProperty(OutputKeys.METHOD, "html")
//    transformer.setOutputProperty(OutputKeys.VERSION, "4.01")

  /**
   * Sanitizes the html in the detailedMessage, to allow only href inside &lt;a&gt; tags.
   *
   * @param detailedMessage The detailedMessage.
   *
   * @return The sanitized detailedMessage.
   */
  private[render] def sanitizeDetailedMessage(detailedMessage: String): String = {
    if (StringUtils.isEmpty(detailedMessage))
      ""
    else if (DETAILED_MESSAGE_A_TAG_REGEXP.matcher(detailedMessage).matches && (!BAD_JAVASCRIPT_REGEXP.matcher(detailedMessage).find))
      detailedMessage
    else
      EscapeUtil.htmlEscape(detailedMessage)
  }

  def renderHtmlError(responseStatus: ResponseStatus): CharSequence = {
    
    import responseStatus._

    val statusDiv = Option(getStatusType) map { s =>
      "<div>Status: %s</div>".format(s.lowerCaseString) } getOrElse ""

    val reasonDiv = Option(getReasonType) map { r =>
      "<div>Reason: %s</div>".format(r.getMessageForReasonType(null)) } getOrElse ""

    val descriptionDiv = Option(getDescription) map { d =>
      "<div>Description: %s</div>".format(sanitizeDetailedMessage(d)) } getOrElse ""

    header +
    "<h3>Oops, an error occured.</h3>" +
    "%s%s%s".format(statusDiv,reasonDiv,descriptionDiv) +
    footer
  }


  private val header = "<html><head><title>Google Visualization</title></head><body>"

  private val footer = "</body></html>"

  private val log = LogFactory getLog "HtmlRenderer"

  /**
   * Pattern for matching against &lt;a&gt; tags with hrefs.
   * Used in sanitizeDetailedMessage.
   */
  private val DETAILED_MESSAGE_A_TAG_REGEXP: Pattern = Pattern.compile("([^<]*<a(( )*target=\"_blank\")*(( )*target='_blank')*" + "(( )*href=\"[^\"]*\")*(( )*href='[^']*')*>[^<]*</a>)+[^<]*")

  /**
   * Pattern for matching against "javascript:".
   * Used in sanitizeDetailedMessage.
   */
  private val BAD_JAVASCRIPT_REGEXP: Pattern = Pattern.compile("javascript(( )*):")
}
