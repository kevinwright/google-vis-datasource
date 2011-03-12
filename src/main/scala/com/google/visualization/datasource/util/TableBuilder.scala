package com.google.visualization.datasource.util

import com.google.common.collect.Lists

import com.google.visualization.datasource.base._
import com.google.visualization.datasource.datatable._
import com.google.visualization.datasource.datatable.value._
import com.google.visualization.datasource.query._
import com.ibm.icu.util.{Calendar,GregorianCalendar,TimeZone}

import java.sql.{Array => _, _ }

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import collection.JavaConversions.{asScalaBuffer, asJavaList}
import GoogleVizImplicits._


object TableBuilder {
  private val log = LogFactory.getLog(this.getClass)

  def build(rs: ResultSet, columnIds: Seq[String]) : DataTable = {
    val table = buildColumns(rs, columnIds)
    log info ("returning " + table.getNumberOfColumns + " columns")
    buildRows(table, rs)
    log info ("built " + table.getRows.length + " rows")
    table
  }

  def colDescToString(col: ColumnDescription) = "%s '%s': %s".format(
    col.getId,
    col.getLabel,
    col.getType)

  def buildColumns(rs: ResultSet, columnIds: Seq[String]) = {
    val metaData: ResultSetMetaData = rs.getMetaData

    log info (columnIds mkString "\n")

    def idOf(idx: Int) = columnIds.lift(idx) getOrElse metaData.getColumnLabel(idx+1)

    val cols = 0 until metaData.getColumnCount map { i =>
      new ColumnDescription(
        idOf(i),
        sqlTypeToValueType(metaData.getColumnType(i+1)),
        metaData.getColumnLabel(i+1)
      )
    }

//    log info (cols map colDescToString mkString "\n")

    val result = new DataTable
    cols foreach { result.addColumn }
    result
  }

  def buildRows(dataTable: DataTable, rs: ResultSet) = {
    val columnsDescriptionList = dataTable.getColumnDescriptions

    // Get the value types of the columns.
    val indexedColumnTypes = columnsDescriptionList map { _.getType } zipWithIndex

    val rowIndices = Iterator.from(0)
    while (rs.next) {
      val rowIdx = rowIndices.next
      val row = new TableRow

      indexedColumnTypes foreach {
        case (ct, colIdx) => row.addCell(buildCell(rs, ct, colIdx, rowIdx));
      }

//      if (rowIdx < 10) {
//        log info rs.getInt(1)
//        log info (row.getCells mkString ",")
//      }

      //hack to prevent an odd bug with null values coming through
      //possibly a lucid-jdbc problem when using getDouble on an int column
      rs.getString(1)

      try dataTable.addRow(row)
      catch { case e: TypeMismatchException => () }
    }

//    log info "first 10 rows:"
//    dataTable.getRows take 10 map { row =>
//      log info (row.getCells mkString ",")
//    }
    dataTable

  }

  /**
   * Creates a table cell from the value in the current row of the given result
   * set and the given column index. The type of the value is determined by the
   * given value type.
   *
   * @param rs The result set holding the data from the sql table. The result
   *     points to the current row.
   * @param valueType The value type of the column that the cell belongs to.
   * @param colIdx The column index. Indexes are 0-based.
   *
   * @return The table cell.
   *
   * @throws SQLException Thrown when the connection to the database failed.
   */

  def buildCell(rs: ResultSet, valueType: ValueType, colIdx: Int, rowIdx: Int): TableCell = {
    import ValueType._
    val column = colIdx + 1
    def newCalendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"))

    def dateToValue(date:Date) = {
      val cal = newCalendar
      cal.setTime(date)
      new DateValue(cal)
    }

    def timestampToValue(timestamp:Timestamp) = {
      val gc = newCalendar
      gc.setTime(timestamp)
      gc.set(Calendar.MILLISECOND, timestamp.getNanos / 1000000)
      new DateTimeValue(gc)
    }

    def timeToValue(time:Time) = {
      val cal = newCalendar
      cal.setTime(time)
      new TimeOfDayValue(cal)
    }

    class NullMappable[T >: Null](obj:T) {
      def nullmap[R >: Null](f : (T) => R) : R = Option(obj) map f orNull
    }
    implicit def anyToNullMappable[T >: Null](obj:T) = new NullMappable(obj)

    val value = if (rs.wasNull) Value.getNullValueFromValueType(valueType)
    else valueType match {
      case BOOLEAN => BooleanValue.getInstance(rs.getBoolean(column))
      case NUMBER => new NumberValue(rs.getDouble(column))
      case DATE => rs.getDate(column) nullmap dateToValue
      case DATETIME => rs.getTimestamp(column) nullmap timestampToValue
      case TIMEOFDAY => rs.getTime(column) nullmap timeToValue
      case _ =>
        if (rs.getString(column) == null) TextValue.getNullValue
        else new TextValue(rs.getString(column))
    }

    new TableCell(value)
  }

  def sqlTypeToValueType(sqlType: Int) = {
    import Types._
    sqlType match {
      case BOOLEAN|BIT => ValueType.BOOLEAN
      case CHAR|VARCHAR => ValueType.TEXT
      case INTEGER|SMALLINT|BIGINT|TINYINT|REAL|NUMERIC|DOUBLE|FLOAT|DECIMAL => ValueType.NUMBER
      case DATE => ValueType.DATE
      case TIME => ValueType.TIMEOFDAY
      case TIMESTAMP => ValueType.DATETIME
      case _ => ValueType.TEXT
    }
  }
}