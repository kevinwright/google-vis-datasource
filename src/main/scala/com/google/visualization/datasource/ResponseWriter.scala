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

import com.google.visualization.datasource.base.{DataSourceParameters, OutputType}
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletResponse

/**
 * A helper class responsible for writing a response message on a <code>HttpServletResponse</code>.
 *
 * @author Nimrod T.
 */
object ResponseWriter {
  /**
   * Sets the specified responseMessage on the given <code>HttpServletResponse</code>.
   * This method assumes the <code>StatusType</code> is 'OK'.
   *
   * @param responseMessage The response message.
   * @param dataSourceParameters The datasource parameters.
   * @param res The HTTP response.
   *
   * @throws IOException In case of a I/O error.
   */
  def setServletResponse(responseMessage: String, dataSourceParameters: DataSourceParameters, res: HttpServletResponse): Unit =
    dataSourceParameters.getOutputType match {
      case OutputType.CSV =>
        setServletResponseCSV(responseMessage, dataSourceParameters, res)
      case OutputType.TSV_EXCEL =>
        setServletResponseTSVExcel(responseMessage, dataSourceParameters, res)
      case OutputType.HTML =>
        setServletResponseHTML(responseMessage, res)
      case OutputType.JSONP =>
        setServletResponseJSON(responseMessage, res)
      case OutputType.JSON =>
        setServletResponseJSON(responseMessage, res)
      case _ =>
        error("Unhandled output type.")
    }

  /**
   * Sets the specified responseMessage on the given <code>HttpServletResponse</code> if
   * the <code>OutputType</code> is CSV.
   * This method assumes the <code>StatusType</code> is 'OK'.
   *
   * @param responseMessage The response message.
   * @param dataSourceParameters The data source parameters.
   * @param res The HTTP response.
   *
   * @throws IOException In case of a I/O error.
   */
  private def setServletResponseCSV(responseMessage: String, dataSourceParameters: DataSourceParameters, res: HttpServletResponse): Unit = {
    res.setContentType("text/csv; charset=UTF-8")
    val outFileName = dataSourceParameters.getOutFileName match {
      case x if !x.toLowerCase.endsWith(".csv") => x + ".csv"
      case x => x
    }
    res.setHeader("content-disposition", "attachment; filename=" + outFileName)
    writeServletResponse(responseMessage, res)
  }

  /**
   * Sets the HTTP servlet response for a TSV_EXCEL output type.
   * This method assumes the <code>StatusType</code> is 'OK'.
   *
   * @param responseMessage The response message.
   * @param dsParams The data source parameters.
   * @param res The HTTP response.
   *
   * @throws IOException In case of a I/O error.
   */
  private def setServletResponseTSVExcel(responseMessage: String, dsParams: DataSourceParameters, res: HttpServletResponse): Unit = {
    res.setContentType("text/csv; charset=UTF-16LE")
    val outFileName = dsParams.getOutFileName
    res.setHeader("Content-Disposition", "attachment; filename=" + outFileName)
    writeServletResponse(responseMessage, res, "UTF-16LE", UTF_16LE_BOM)
  }

  /**
   * Sets the HTTP servlet response for a HTML output type.
   * This method assumes the <code>StatusType</code> is 'OK'.
   *
   * @param responseMessage The response message.
   * @param res The HTTP response.
   *
   * @throws IOException In case of a I/O error.
   */
  private def setServletResponseHTML(responseMessage: String, res: HttpServletResponse): Unit = {
    res.setContentType("text/html; charset=UTF-8")
    writeServletResponse(responseMessage, res)
  }

  /**
   * Sets the HTTP servlet response for a JSON output type.
   * This method assumes the <code>StatusType</code> is 'OK'.
   *
   * @param responseMessage The response char sequence.
   * @param res The HTTP response.
   *
   * @throws IOException In case of a I/O error.
   */
  private def setServletResponseJSON(responseMessage: String, res: HttpServletResponse): Unit = {
    res.setContentType("text/plain; charset=UTF-8")
    writeServletResponse(responseMessage, res)
  }

  /**
   * Writes the response to the servlet response using UTF-8 charset without
   * byte-order mark.
   *
   * @param responseMessage A charSequence to write to the servlet response.
   * @param res The servlet response.
   *
   * @throws IOException In case of a I/O error.
   */
  private def writeServletResponse(responseMessage: CharSequence, res: HttpServletResponse): Unit = {
    writeServletResponse(responseMessage, res, "UTF-8", null)
  }

  /**
   * Writes the response to the servlet response using specified charset and an
   * optional byte-order mark.
   *
   * @param charSequence A charSequence to write to the servlet response.
   * @param res The servlet response.
   * @param charset A {@code String} specifying one of the character sets
   *        defined by IANA Character Sets
   *        (http://www.iana.org/assignments/character-sets).
   * @param byteOrderMark An optional byte-order mark.
   *
   * @throws IOException In case of a I/O error.
   */
  private def writeServletResponse(charSequence: CharSequence, res: HttpServletResponse, charset: String, byteOrderMark: Array[Byte]): Unit = {
    var outputStream: ServletOutputStream = res.getOutputStream
    if (byteOrderMark != null) {
      outputStream write byteOrderMark
    }
    outputStream write charSequence.toString.getBytes(charset)
  }

  /**
   * UTF-16LE byte-order mark required by TSV_EXCEL output type.
   * @see OutputType#TSV_EXCEL
   */
  private final val UTF_16LE_BOM: Array[Byte] = Array(0xff, 0xfe) map (_.asInstanceOf[Byte])
}
