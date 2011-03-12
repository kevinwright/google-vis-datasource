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
package com.google.visualization.datasource.base

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

/**
 * This class contains the data source parameters of the request.
 * The data source parameters are extracted from the "tqx" URL parameter.
 *
 * @author Nimrod T.
 */
object DataSourceParameters {
  /**
   * Returns a default DataSourceParameters object.
   *
   * @return A default DataSourceParameters object.
   */
  def getDefaultDataSourceParameters: DataSourceParameters = try {
    new DataSourceParameters(null)
  } catch { case e: DataSourceException => null }

  /**
   * The name of the request id parameter as it appears in the "tqx" URL parameter.
   */
  val REQUEST_ID_PARAM_NAME = "reqId"
  /**
   * The name of the data signature parameter as it appears in the "tqx" URL parameter.
   */
  val SIGNATURE_PARAM_NAME = "sig"
  /**
   * The name of the output type parameter as it appears in the "tqx" URL parameter.
   */
  val OUTPUT_TYPE_PARAM_NAME = "out"
  /**
   * The name of the response handler parameter as it appears in the "tqx" URL parameter.
   */
  val RESPONSE_HANDLER_PARAM_NAME = "responseHandler"
  /**
   * The name of the filename parameter as it appears in the "tqx" URL parameter.
   * This is relevant only if the "out" parameter is "csv".
   * In that case, if the outFileName is given,
   * this is the name of the csv that the datasource will output.
   * Otherwise, if missing, the default filename (data.csv) is used.
   */
  val REQUEST_OUTFILENAME_PARAM_NAME = "outFileName"
  /**
   * A default error message.
   */
  val DEFAULT_ERROR_MSG = "Internal error"
}

import DataSourceParameters._


class DataSourceParameters {
  /**
   * Log.
   */
  val log = LogFactory.getLog(this.getClass)

  /**
   * Constructs a new instance of this class, with the given "tqx" string and parses it.
   *
   * Note: the "tqx" parameter is generated internally by the client side
   * libraries. Thus, both the user and the developer will receive only short
   * error messages regarding it. E.g., "Internal Error (code)".
   *
   * @param tqxValue The "tqx" string to parse.
   * @throws DataSourceException Thrown if parsing of request parameters fails.
   */
  def this(tqxValue: String) {
    this ()
    if (StringUtils isNotEmpty tqxValue) {
      this.tqxValue = tqxValue
      val parts = tqxValue split ";"

      val partPairs = parts map { part =>
        val pair = part split ":"
        if (pair.length != 2) {
          log.error("Invalid name-value pair: " + part)
          throw new DataSourceException(ReasonType.INVALID_REQUEST, DEFAULT_ERROR_MSG + "(malformed)")
        }
        val Array(name, value) = pair
        name -> value
      }

      partPairs foreach { case (name, value) =>
        name match {
          case REQUEST_ID_PARAM_NAME => requestId = value
          case SIGNATURE_PARAM_NAME => signature = value
          case OUTPUT_TYPE_PARAM_NAME =>
            outputType = OutputType.findByCode(value)
            if (outputType == null) {
              outputType = OutputType.defaultValue
            }
          case RESPONSE_HANDLER_PARAM_NAME => responseHandler = value
          case REQUEST_OUTFILENAME_PARAM_NAME =>
            outFileName = value
            if (!outFileName.contains(".")) {
              outFileName += ".csv"
            }
          case _ =>
        }
      }
    }
  }

  /**
   * Returns the request id.
   *
   * @return The request id.
   */
  def getRequestId: String = {
    return requestId
  }

  /**
   * Returns the signature.
   *
   * @return The signature.
   */
  def getSignature: String = {
    return signature
  }

  /**
   * Sets the signature.
   *
   * @param signature The signature.
   */
  def setSignature(signature: String): Unit = {
    this.signature = signature
  }

  /**
   * Returns the output type.
   *
   * @return The output type.
   */
  def getOutputType: OutputType = {
    return outputType
  }

  /**
   * Sets the output type.
   *
   * @param outputType The output type.
   */
  def setOutputType(outputType: OutputType): Unit = {
    this.outputType = outputType
  }

  /**
   * Returns the response handler.
   *
   * @return The response handler.
   */
  def getResponseHandler: String = {
    return responseHandler
  }

  /**
   * Returns the out file name.
   *
   * @return The out file name.
   */
  def getOutFileName: String = {
    return outFileName
  }

  /**
   * Returns the "tqx" value.
   *
   * @return The "tqx" value.
   */
  def getTqxValue: String = {
    return tqxValue
  }

  /**
   * The original string passed as the value of the "tqx" parameter.
   */
  private var tqxValue: String = null
  /**
   * The request id.
   */
  private var requestId: String = null
  /**
   * The signature.
   */
  private var signature: String = null
  /**
   * The output type.
   */
  private var outputType: OutputType = OutputType.defaultValue
  /**
   * The response handler.
   */
  private var responseHandler: String = "google.visualization.Query.setResponse"
  /**
   * The out filename.
   */
  private var outFileName: String = "data.csv"
}

