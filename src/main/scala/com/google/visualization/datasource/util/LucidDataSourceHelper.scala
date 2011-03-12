package com.google.visualization.datasource
package util

import com.google.common.collect.Lists
import base._
import datatable._
import value._
import query._
import org.apache.commons.lang.StringUtils

import org.apache.commons.logging.LogFactory
import java.sql.{Array => _, _ }
import collection.JavaConversions.{asScalaBuffer, asJavaList}

import GoogleVizImplicits._


object LucidDataSourceHelper {

  private val log = LogFactory.getLog(this.getClass)

  def executeQuery(query: Query, dbDescription: SqlDatabaseDescription, tableName: String): DataTable = {
    type Closable = { def close() : Unit }

    def safelyClose(item : Closable) : Unit = {
      try Option(item).map(_.close)
      catch { case e: SQLException => () }
    }

    def withQuery[T](con: Connection, query: String)(body: ResultSet => T) = {
      var stmt: Statement = null
      try {
        stmt = con.createStatement
        log info "connected, now running query"
        body(stmt.executeQuery(query))
      } catch {
        case e: SQLException =>
          throw new DataSourceException(
            ReasonType.INTERNAL_ERROR,
            "Failed to execute SQL query. mySQL error message: " + e.getMessage
          )
      } finally {
        safelyClose(stmt)
        safelyClose(con)
      }
    }

    def getColumnIds(qs: QuerySelection) = qs.getColumns map (_.getId)

    log info ("Running Lucid query")
    val queryStr = SqlBuilder(query, tableName).build
    log info ("query string is: " + queryStr)


//    val columnIds = query.selectionOpt map getColumnIds getOrElse Seq.empty
    val columnIds = (query.selectionOpt map getColumnIds).flatten.toSeq

    withQuery(getDatabaseConnection(dbDescription), queryStr) { rs => TableBuilder.build(rs, columnIds) }
  }


  private def getDatabaseConnection(desc: SqlDatabaseDescription): Connection = {
    try {
      DriverManager.getConnection(desc.getUrl, desc.getUser, desc.getPassword)
    } catch {
      case e: SQLException => {
        log.error("Failed to connect to database server.", e)
        throw new DataSourceException(ReasonType.INTERNAL_ERROR, "Failed to connect to database server.")
      }
    }
  }

  class SqlBuilder(query: Query, tableName: String) {

    def bracket(str: String): String = '(' +: str :+ ')'
    def quoteCol(str: String): String = '"' +: str :+ '"'
    def quote(str: String): String = '\'' +: str :+ '\''
    def unquote(str: String): String = str.filter(_ != '\'')
    def requote(left: String, str: String, right: String) = quote(left + unquote(str) + right)

    def build =
      selectClause +
      fromClause +
      whereClause +
      groupByClause +
      orderByClause +
      limitAndOffsetClause

    def selectClause =
      query.selectionOpt map {
        _.getColumns map getColumnId mkString("SELECT ", ", ", " ")
      } getOrElse "SELECT * "

    def fromClause =
      if (StringUtils.isEmpty(tableName)) {
        log.error("No table name provided.")
        throw new DataSourceException(ReasonType.OTHER, "No table name provided.")
      } else "FROM " + tableName + " "

    def whereClause =
      query.filterOpt map { f => "WHERE " + buildWhereClauseFor(f) + " " } getOrElse ""

    def groupByClause =
      query.groupOpt map {
        _.getColumnIds map quoteCol mkString("GROUP BY ", ", ", " ")
      } getOrElse ""

    def orderByClause =
      query.sortOpt map { sort =>
        def columnId(cs: ColumnSort) = getColumnId(cs.getColumn)
        def columnOrdering(cs: ColumnSort) =if (cs.getOrder == SortOrder.DESCENDING) " DESC" else ""
        def idWithOrdering(cs: ColumnSort) = columnId(cs) + columnOrdering(cs)

        sort.getSortColumns map idWithOrdering mkString("ORDER BY ", ", ", " ")
      } getOrElse ""

    def limitAndOffsetClause =
      (query.rowLimitOpt map ("LIMIT " + _) getOrElse "") +
      (query.rowOffsetOpt map (" OFFSET " + _) getOrElse "")


    def buildWhereClauseFor(queryFilter: QueryFilter) : String = {
      queryFilter match {
        case filter: ColumnIsNullFilter => "(" + filter.getColumn.getId + " IS NULL)"
        case filter: ComparisonFilter => whereClauseForComparisonFilter(filter)
        case filter: NegationFilter => "(NOT " + buildWhereClauseFor(filter.getSubFilter) + ")"
        case filter: CompoundFilter =>
          import filter._
          if (getSubFilters.isEmpty) {
             if (getOperator == CompoundFilterLogicalOperator.AND) "true" else "false"
          } else {
            val logicalOperator = " " + getSqlLogicalOperator(getOperator) + " "
            getSubFilters map buildWhereClauseFor mkString("(", logicalOperator, ")")
          }
      }
    }

    def whereClauseForComparisonFilter(filter: ComparisonFilter) = {
      def isQuotable(tpe: ValueType) =
        tpe == ValueType.TEXT ||
        tpe == ValueType.DATE ||
        tpe == ValueType.DATETIME ||
        tpe == ValueType.TIMEOFDAY

      def quoteIfNeeded(v: Value) =
        if(isQuotable(v.getType)) quote(v.toString) else v.toString

      val (lhs, rhs) = filter match {
        case ccf: ColumnColumnFilter => (ccf.getFirstColumn.getId, ccf.getSecondColumn.getId)
        case cvf : ColumnValueFilter => (cvf.getColumn.getId, quoteIfNeeded(cvf.getValue))
      }

      import ComparisonFilterOperator._

      bracket(filter.getOperator match {
        case EQ => lhs + "=" + rhs
        case NE => lhs + "<>" + rhs
        case LT => lhs + "<" + rhs
        case GT => lhs + ">" + rhs
        case LE => lhs + "<=" + rhs
        case GE => lhs + ">=" + rhs
        case CONTAINS => lhs + " LIKE " + requote("%", rhs, "%")
        case STARTS_WITH => lhs + " LIKE " + requote("", rhs, "%")
        case ENDS_WITH => lhs + " LIKE " + requote("%", rhs, "")
        case LIKE => lhs + " LIKE " + requote("", rhs, "")
        case MATCHES => error("SQL does not support regular expression")
        case operator => error("Operator was not found: " + operator)
      })
    }

    def getColumnId(abstractColumn: AbstractColumn) = abstractColumn match {
      case sc: SimpleColumn => quoteCol(sc.getId)
      case ac: AggregationColumn =>
        ac.getAggregationType.getCode + bracket(quoteCol(ac.getAggregatedColumn.toString))
    }

    def getSqlLogicalOperator(operator: CompoundFilterLogicalOperator) = operator match {
      case CompoundFilterLogicalOperator.AND => "AND"
      case CompoundFilterLogicalOperator.OR => "OR"
      case _ => error("Logical operator was not found: " + operator)
    }

  }

  object SqlBuilder {
    def apply (query: Query, tableName: String) = new SqlBuilder(query, tableName)
  }
}


