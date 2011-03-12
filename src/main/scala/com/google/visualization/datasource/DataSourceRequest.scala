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

import com.google.visualization.datasource.base.DataSourceException
import com.google.visualization.datasource.base.DataSourceParameters
import com.google.visualization.datasource.base.InvalidQueryException
import com.google.visualization.datasource.base.OutputType
import com.google.visualization.datasource.query.Query
import com.ibm.icu.util.ULocale
import javax.servlet.http.HttpServletRequest

/**
 * This class contains all information concerning a data source request. The information in this
 * class is used to create a data table and return a corresponding response to the user.
 *
 * @author Yaniv S.
 *
 */
object DataSourceRequest {
  /**
   * Returns a default data source request.
   *
   * Used in case no data source request is available (i.e., because of invalid input).
   * Parses the input 'tq' and 'tqx' strings and, if it fails, uses default values.
   *
   * @param req The http servlet request.
   *
   * @return A default data source request.
   */
  def getDefaultDataSourceRequest(req: HttpServletRequest) = {
    var dataSourceRequest = new DataSourceRequest
    dataSourceRequest.inferLocaleFromRequest(req)
    dataSourceRequest.sameOrigin = determineSameOrigin(req)
    try {
      dataSourceRequest.createDataSourceParametersFromRequest(req)
    } catch {
      case e: DataSourceException => {
        if (dataSourceRequest.dsParams == null) {
          dataSourceRequest.dsParams = DataSourceParameters.getDefaultDataSourceParameters
        }
        if ((dataSourceRequest.dsParams.getOutputType == OutputType.JSON) && (!dataSourceRequest.sameOrigin)) {
          dataSourceRequest.dsParams.setOutputType(OutputType.JSONP)
        }
      }
    }

    try {
      dataSourceRequest.createQueryFromRequest(req)
    } catch {
      case e: InvalidQueryException =>
    }
    dataSourceRequest
  }

  /**
   * Determines whether the given request is from the same origin (based on
   * request headers set by the ).
   *
   * @param req The http servlet request.
   *
   * @return True if this is a same origin request false otherwise.
   */
  def determineSameOrigin(req: HttpServletRequest) =
    req.getHeader(SAME_ORIGIN_HEADER) != null

  /**
   * A request header name. Used to determine if the request was sent from the same domain as the
   * server.
   */
  val SAME_ORIGIN_HEADER = "X-DataSource-Auth"
  /**
   * The name of the request parameter that specifies the query to execute.
   */
  val QUERY_REQUEST_PARAMETER = "tq"
  /**
   * The name of the request parameter that specifies the data source parameters.
   */
  val DATASOURCE_REQUEST_PARAMETER = "tqx"
}

class DataSourceRequest private {
  import DataSourceRequest._
  
  /**
   * The query (based on the "tq" request parameter).
   */
  private var query: Query = null
  /**
   * The data source parameters (based on the "tqx" request parameter).
   */
  private var dsParams: DataSourceParameters = null
  /**
   * The user locale.
   */
  private var userLocale: ULocale = null
  /**
   * Indicates whether the request is from the same origin.
   */
  private var sameOrigin: Boolean = false


  /**
   * Constructor.
   * Useful for debugging and testing.
   * Do not use it for production.
   *
   * @param query The query.
   * @param dsParams The data source parameters.
   * @param userLocale The user locale.
   */
  def this(query: Query, dsParams: DataSourceParameters, userLocale: ULocale) {
    this()
    setUserLocale(userLocale)
    this.dsParams = dsParams
    this.query = query
  }

  /**
   * Builds a DataSource request from an <code>HttpServletRequest</code>.
   *
   * @param req The HttpServletRequest.
   *
   * @throws DataSourceException In case of an invalid 'tq' or 'tqx' parameter.
   */
  def this(req: HttpServletRequest) {
    this()
    inferLocaleFromRequest(req)
    sameOrigin = determineSameOrigin(req)
    createDataSourceParametersFromRequest(req)
    createQueryFromRequest(req)
  }

  /**
   * Creates the <code>Query</code> based on the 'tq' parameter on the given request.
   *
   * @param req The http servlet request.
   *
   * @throws InvalidQueryException if the 'tq' string is invalid or missing on the request.
   */
  private def createQueryFromRequest(req: HttpServletRequest): Unit = {
    DataSourceHelper.parseQuery(req.getParameter(QUERY_REQUEST_PARAMETER))
  }

  /**
   * Creates the <code>DataSourceParameters</code> based on the 'tqx' parameter on the given
   * request.
   *
   * @param req The http servlet request.
   *
   * @throws DataSourceException Thrown if the data source parameters or query could not be parsed.
   */
  private def createDataSourceParametersFromRequest(req: HttpServletRequest): Unit = {
    val dataSourceParamsString = req.getParameter(DATASOURCE_REQUEST_PARAMETER)
    dsParams = new DataSourceParameters(dataSourceParamsString)
    if (dsParams.getOutputType == OutputType.JSON && !sameOrigin) {
      dsParams.setOutputType(OutputType.JSONP)
    }
  }

  /**
   * Infers the locale from the http servlet request.
   *
   * @param req The http servlet request.
   */
  private def inferLocaleFromRequest(req: HttpServletRequest): Unit = {
    userLocale = DataSourceHelper getLocaleFromRequest req
  }

  /**
   * Returns the query.
   *
   * @return The query.
   */
  def getQuery = query

  /**
   * Returns the data source parameters.
   *
   * @return The data source parameters.
   */
  def getDataSourceParameters = dsParams

  /**
   * Sets the user locale.
   *
   * @param userLocale The user locale.
   */
  def setUserLocale(userLocale: ULocale): Unit = this.userLocale = userLocale

  /**
   * Returns the user locale.
   *
   * @return The user locale.
   */
  def getUserLocale = userLocale

  /**
   * Returns true if the request is same-origin.
   *
   * @return True if the request is same-origin.
   */
  def isSameOrigin = sameOrigin

}


