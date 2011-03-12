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

import base.DataSourceException
import base.InvalidQueryException
import base.LocaleUtil
import base.MessagesEnum
import base.OutputType
import base.ReasonType
import base.ResponseStatus
import base.StatusType
import datatable.DataTable
import query.AggregationColumn
import query.Query
import query.ScalarFunctionColumn
import query.engine.QueryEngine
import query.parser.QueryBuilder
import render.CsvRenderer
import render.HtmlRenderer
import render.JsonRenderer
import com.ibm.icu.util.ULocale
import org.apache.commons.logging.LogFactory
import java.util.Set
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import collection.JavaConverters._

/**
 * A Helper class providing convenience functions for serving data source requests.
 *
 * The class enables replying to a data source request with a single method which encompasses
 * all the request processing - <code>executeDataSourceServletFlow</code>.
 * To enable users to change the default flow all the basic operations (such as: query parsing,
 * data table creation, query execution, and response creation) are also exposed.
 *
 * @author Yaniv S.
 */
object DataSourceHelper {
  /**
   * Executes the default data source servlet flow.
   * Assumes restricted access mode.
   * @see <code>executeDataSourceServletFlow(HttpServletRequest req, HttpServletResponse resp,
   *     DataTableGenerator dtGenerator, boolean isRestrictedAccessMode)</code>
   *
   * @param req The HttpServletRequest.
   * @param resp The HttpServletResponse.
   * @param dtGenerator An implementation of {@link DataTableGenerator} interface.
   *
   * @throws IOException In case of I/O errors.
   */
  def executeDataSourceServletFlow(req: HttpServletRequest, resp: HttpServletResponse, dtGenerator: DataTableGenerator): Unit = {
    executeDataSourceServletFlow(req, resp, dtGenerator, true)
  }

  /**
   * Executes the default data source servlet flow.
   *
   * The default flow is as follows:
   * - Parse the request parameters.
   * - Verify access is approved (for restricted access mode only).
   * - Split the query.
   * - Generate the data-table using the data-table generator.
   * - Run the completion query.
   * - Set the servlet response.
   *
   * Usage note : this function executes the same flow provided to Servlets that inherit
   * <code>DataSourceServlet</code>.
   * Use this function when the default flow is required but <code>DataSourceServlet</code>
   * cannot be inherited (e.g., your servlet already inherits from anther class, or not in a
   * servlet context).
   *
   * @param req The HttpServletRequest.
   * @param resp The HttpServletResponse.
   * @param dtGenerator An implementation of {@link DataTableGenerator} interface.
   * @param isRestrictedAccessMode Indicates whether the server should serve trusted domains only.
   *     Currently this translates to serving only requests from the same domain.
   *
   * @throws IOException In case of I/O errors.
   */
  def executeDataSourceServletFlow(req: HttpServletRequest, resp: HttpServletResponse, dtGenerator: DataTableGenerator, isRestrictedAccessMode: Boolean): Unit = {
    var dsRequest: DataSourceRequest = null
    try {
      dsRequest = new DataSourceRequest(req)
      if (isRestrictedAccessMode) {
        DataSourceHelper.verifyAccessApproved(dsRequest)
      }
      var query: QueryPair = DataSourceHelper.splitQuery(dsRequest.getQuery, dtGenerator.getCapabilities)
      var dataTable: DataTable = dtGenerator.generateDataTable(query.getDataSourceQuery, req)
      var newDataTable: DataTable = DataSourceHelper.applyQuery(query.getCompletionQuery, dataTable, dsRequest.getUserLocale)
      setServletResponse(newDataTable, dsRequest, resp)
    }
    catch {
      case e: DataSourceException => {
        if (dsRequest != null) setServletErrorResponse(e, dsRequest, resp)
        else DataSourceHelper.setServletErrorResponse(e, req, resp)
      }
      case e: RuntimeException => {
        log.error("A runtime exception has occured", e)
        var status: ResponseStatus = new ResponseStatus(StatusType.ERROR, ReasonType.INTERNAL_ERROR, e.getMessage)
        if (dsRequest == null) {
          dsRequest = DataSourceRequest.getDefaultDataSourceRequest(req)
        }
        DataSourceHelper.setServletErrorResponse(status, dsRequest, resp)
      }
    }
  }

  /**
   * Checks that the given request is sent from the same domain as that of the server.
   *
   * @param req The data source request.
   *
   * @throws DataSourceException If the access for this request is denied.
   */
  def verifyAccessApproved(req: DataSourceRequest): Unit = {
    val outType = req.getDataSourceParameters.getOutputType
    if (outType != OutputType.CSV && outType != OutputType.TSV_EXCEL && outType != OutputType.HTML && !req.isSameOrigin) {
      throw new DataSourceException(ReasonType.ACCESS_DENIED, "Unauthorized request. Cross domain requests are not supported.")
    }
  }

  /**
   * Sets the response on the <code>HttpServletResponse</code> by creating a response message
   * for the given <code>DataTable</code> and sets it on the <code>HttpServletResponse</code>.
   *
   * @param dataTable The data table.
   * @param dataSourceRequest The data source request.
   * @param res The http servlet response.
   *
   * @throws IOException In case an error happened trying to write the response to the servlet.
   */
  def setServletResponse(dataTable: DataTable, dataSourceRequest: DataSourceRequest, res: HttpServletResponse): Unit = {
    val responseMessage = generateResponse(dataTable, dataSourceRequest)
    setServletResponse(responseMessage, dataSourceRequest, res)
  }

  /**
   * Sets the given response string on the <code>HttpServletResponse</code>.
   *
   * @param responseMessage The response message.
   * @param dataSourceRequest The data source request.
   * @param res The HTTP response.
   *
   * @throws IOException In case an error happened trying to write to the servlet response.
   */
  def setServletResponse(responseMessage: String, dataSourceRequest: DataSourceRequest, res: HttpServletResponse): Unit = {
    val dataSourceParameters = dataSourceRequest.getDataSourceParameters
    ResponseWriter.setServletResponse(responseMessage, dataSourceParameters, res)
  }

  /**
   * Sets the HTTP servlet response in case of an error.
   *
   * @param dataSourceException The data source exception.
   * @param dataSourceRequest The data source request.
   * @param res The http servlet response.
   *
   * @throws IOException In case an error happened trying to write the response to the servlet.
   */
  def setServletErrorResponse(dataSourceException: DataSourceException, dataSourceRequest: DataSourceRequest, res: HttpServletResponse): Unit = {
    val responseMessage = generateErrorResponse(dataSourceException, dataSourceRequest)
    setServletResponse(responseMessage, dataSourceRequest, res)
  }

  /**
   * Sets the HTTP servlet response in case of an error.
   *
   * @param responseStatus The response status.
   * @param dataSourceRequest The data source request.
   * @param res The http servlet response.
   *
   * @throws IOException In case an error happened trying to write the response to the servlet.
   */
  def setServletErrorResponse(responseStatus: ResponseStatus, dataSourceRequest: DataSourceRequest, res: HttpServletResponse): Unit = {
    val responseMessage = generateErrorResponse(responseStatus, dataSourceRequest)
    setServletResponse(responseMessage, dataSourceRequest, res)
  }

  /**
   * Sets the HTTP servlet response in case of an error.
   *
   * Gets an <code>HttpRequest</code> parameter instead of a <code>DataSourceRequest</code>.
   * Use this when <code>DataSourceRequest</code> is not available, for example, if
   * <code>DataSourceRequest</code> constructor failed.
   *
   * @param dataSourceException The data source exception.
   * @param req The http servlet request.
   * @param res The http servlet response.
   *
   * @throws IOException In case an error happened trying to write the response to the servlet.
   */
  def setServletErrorResponse(dataSourceException: DataSourceException, req: HttpServletRequest, res: HttpServletResponse): Unit = {
    val dataSourceRequest = DataSourceRequest.getDefaultDataSourceRequest(req)
    setServletErrorResponse(dataSourceException, dataSourceRequest, res)
  }

  /**
   * Generates a string response for the given <code>DataTable</code>.
   *
   * @param dataTable The data table.
   * @param dataSourceRequest The data source request.
   *
   * @return The response string.
   */
  def generateResponse(dataTable: DataTable, dataSourceRequest: DataSourceRequest): String = {
    var responseStatus: ResponseStatus = null
    if (!dataTable.getWarnings.isEmpty) {
      responseStatus = new ResponseStatus(StatusType.WARNING)
    }
    val response = dataSourceRequest.getDataSourceParameters.getOutputType match {
      case OutputType.CSV =>
        CsvRenderer.renderDataTable(dataTable, dataSourceRequest.getUserLocale, ",")
      case OutputType.TSV_EXCEL =>
        CsvRenderer.renderDataTable(dataTable, dataSourceRequest.getUserLocale, "\t")
      case OutputType.HTML =>
        HtmlRenderer.renderDataTable(dataTable, dataSourceRequest.getUserLocale)
      case OutputType.JSONP =>
        JsonRenderer.renderJsonResponse(dataSourceRequest.getDataSourceParameters, responseStatus, dataTable)
      case OutputType.JSON =>
        JsonRenderer.renderJsonResponse(dataSourceRequest.getDataSourceParameters, responseStatus, dataTable)
      case _ =>
        throw new RuntimeException("Unhandled output type.")
    }
    response.toString
  }

  /**
   * Generates an error response string for the given {@link DataSourceException}.
   * Receives an exception, and renders it to an error response according to the
   * {@link OutputType} specified in the {@link DataSourceRequest}.
   *
   * Note: modifies the response status to make links clickable in cases where the reason type is
   * {@link ReasonType#USER_NOT_AUTHENTICATED}. If this is not required call generateErrorResponse
   * directly with a {@link ResponseStatus}.
   *
   * @param dse The data source exception.
   * @param dsRequest The DataSourceRequest.
   *
   * @return The error response string.
   *
   * @throws IOException In case if I/O errors.
   */
  def generateErrorResponse(dse: DataSourceException, dsRequest: DataSourceRequest): String = {
    val responseStatus = ResponseStatus.getModifiedResponseStatus(
      ResponseStatus createResponseStatus dse
    )
    generateErrorResponse(responseStatus, dsRequest)
  }

  /**
   * Generates an error response string for the given <code>ResponseStatus</code>.
   * Render the <code>ResponseStatus</code> to an error response according to the
   * <code>OutputType</code> specified in the <code>DataSourceRequest</code>.
   *
   * @param responseStatus The response status.
   * @param dsRequest The DataSourceRequest.
   *
   * @return The error response string.
   *
   * @throws IOException In case if I/O errors.
   */
  def generateErrorResponse(responseStatus: ResponseStatus, dsRequest: DataSourceRequest): String = {
    val dsParameters = dsRequest.getDataSourceParameters
    val response = dsParameters.getOutputType match {
      case OutputType.CSV =>
      case OutputType.TSV_EXCEL =>
        CsvRenderer.renderCsvError(responseStatus)
      case OutputType.HTML =>
        HtmlRenderer.renderHtmlError(responseStatus)
      case OutputType.JSONP =>
        JsonRenderer.renderJsonResponse(dsParameters, responseStatus, null)
      case OutputType.JSON =>
        JsonRenderer.renderJsonResponse(dsParameters, responseStatus, null)
      case _ =>
        throw new RuntimeException("Unhandled output type.")
    }
    response.toString
  }

  /**@see #parseQuery(String, ULocale)*/
  def parseQuery(queryString: String): Query = parseQuery(queryString, null)

  /**
   * Parses a query string (e.g., 'select A,B pivot B') and creates a Query object.
   * Throws an exception if the query is invalid.
   *
   * @param queryString The query string.
   * @param locale The user locale.
   *
   * @return The parsed query object.
   *
   * @throws InvalidQueryException If the query is invalid.
   */
  def parseQuery(queryString: String, userLocale: ULocale): Query = {
    val queryBuilder = QueryBuilder.getInstance
    queryBuilder.parseQuery(queryString, userLocale)
  }

  /**
   * Applies the given <code>Query</code> on the given <code>DataTable</code> and returns the
   * resulting <code>DataTable</code>. This method may change the given DataTable.
   * Error messages produced by this method will be localized according to the passed locale 
   * unless the specified {@code DataTable} has a non null locale. 
   *
   * @param query The query object.
   * @param dataTable The data table on which to apply the query.
   * @param locale The user locale for the current request.
   *
   * @return The data table result of the query execution over the given data table.
   *
   * @throws InvalidQueryException If the query is invalid.
   * @throws DataSourceException If the data source cannot execute the query.
   */
  def applyQuery(query: Query, dataTable: DataTable, locale: ULocale) = {
    dataTable setLocaleForUserMessages locale
    validateQueryAgainstColumnStructure(query, dataTable)

    var dt = QueryEngine.executeQuery(query, dataTable, locale)
    dt setLocaleForUserMessages locale
    dt
  }

  /**
   * Splits the <code>Query</code> object into two queries according to the declared data source
   * capabilities: data source query and completion query.
   *
   * The data source query is executed first by the data source itself. Afterward, the
   * <code>QueryEngine</code> executes the completion query over the resulting data table.
   *
   * @param query The query to split.
   * @param capabilities The declared capabilities of the data source.
   *
   * @return A QueryPair object.
   *
   * @throws DataSourceException If the query cannot be split.
   */
  def splitQuery(query: Query, capabilities: Capabilities) =
    QuerySplitter.splitQuery(query, capabilities)

  /**
   * Checks that the query is valid against the structure of the data table.
   * A query is invalid if:
   * <ol>
   * <li> The query references column ids that don't exist in the data table.
   * <li> The query contains calculated columns operations (i.e., scalar function, aggregations)
   * that do not match the relevant columns type.
   * </ol>
   *
   * Note: does NOT validate the query itself, i.e. errors like "SELECT a, a" or
   * "SELECT a GROUP BY a" will not be caught. These kind of errors should be checked elsewhere
   * (preferably by the <code>Query.validate()</code> method).
   *
   * @param query The query to check for validity.
   * @param dataTable The data table against which to validate. Only the columns are used.
   *
   * @throws InvalidQueryException Thrown if the query is found to be invalid
   *     against the given data table.
   */
  def validateQueryAgainstColumnStructure(query: Query, dataTable: DataTable): Unit = {
    var mentionedColumnIds = query.getAllColumnIds.asScala
    for (columnId <- mentionedColumnIds) {
      if (!dataTable.containsColumn(columnId)) {
        var messageToLogAndUser: String = MessagesEnum.NO_COLUMN.getMessageWithArgs(dataTable.getLocaleForUserMessages, columnId)
        log.error(messageToLogAndUser)
        throw new InvalidQueryException(messageToLogAndUser)
      }
    }
    var mentionedAggregations = query.getAllAggregations.asScala
    for (agg <- mentionedAggregations) {
      try {
        agg.validateColumn(dataTable)
      }
      catch {
        case e: RuntimeException => {
          log.error("A runtime exception has occured", e)
          throw new InvalidQueryException(e.getMessage)
        }
      }
    }
    var mentionedScalarFunctionColumns = query.getAllScalarFunctionsColumns.asScala
    for (col <- mentionedScalarFunctionColumns) {
      col.validateColumn(dataTable)
    }
  }

  /**
   * Get the locale from the given request.
   *
   * @param req The http serlvet request
   *
   * @return The locale for the given request.
   */
  def getLocaleFromRequest(req: HttpServletRequest): ULocale = {
    var requestLocale = req.getParameter(LOCALE_REQUEST_PARAMETER)
    val locale = if (requestLocale != null) {
      LocaleUtil.getLocaleFromLocaleString(requestLocale)
    } else {
      req.getLocale
    }
    ULocale forLocale locale
  }

  /**
   * The log used throughout the data source library.
   */
  private final val log = LogFactory.getLog(DataSourceHelper.getClass.getName)
  /**
   * The name of the http request parameter that indicates the requested locale.
   */
  private[datasource] final val LOCALE_REQUEST_PARAMETER = "hl"
}
