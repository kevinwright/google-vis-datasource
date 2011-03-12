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
package util

import com.google.common.collect.Lists
import base.DataSourceException
import base.ReasonType
import base.TypeMismatchException
import datatable.ColumnDescription
import datatable.DataTable
import datatable.TableRow
import datatable.ValueFormatter
import datatable.value.Value
import datatable.value.ValueType
import au.com.bytecode.opencsv.CSVReader
import com.ibm.icu.util.ULocale
import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.Reader
import java.net.MalformedURLException
import java.net.URL
import java.util.ArrayList
import java.util.List
import java.util.Map

/**
 * Helper class with static utility methods that are specific for building a
 * data source based on CSV (Comma Separated Values) files.
 * The main functionality is taking a CSV and generating a data table.
 *
 * @author Nimrod T.
 */
object CsvDataSourceHelper {
  /**
   * @see #read(java.io.Reader, java.util.List, Boolean, ULocale)
   */
  def read(reader: Reader, columnDescriptions: List[ColumnDescription], headerRow: Boolean): DataTable = {
    read(reader, columnDescriptions, headerRow, null)
  }

  /**
   * Translates a CSV formatted input into a data table representation.
   * The reader parameter should point to a CSV file.
   *
   * @param reader The CSV input Reader from which to read.
   * @param columnDescriptions The column descriptions.
   *     If columnDescriptions is null, then it is assumed that all 
   *     values are strings, and the number of the columns is equal
   *     to the number of the columns in the first line of the reader.
   *     If headerRow is set to true, and the columnDescriptions does not
   *     contain the labels of the columns, then the headerRow values are
   *     used as the column labels.
   * @param headerRow True if there is an header row.
   *     In that case, the first line of the csv is taken as the header row.
   * @param locale An optional locale in which to parse the input csv file.
   *     If null, uses the default from {@code LocaleUtil#getDefaultLocale}.
   *
   * @return A data table with the values populated from the CSV file.
   *
   * @throws IOException In case of error reading from the reader.
   * @throws CsvDataSourceException In case of specific csv error.
   */
  def read(reader: Reader, columnDescriptions0: List[ColumnDescription], headerRow: Boolean, locale: ULocale): DataTable = {
    var columnDescriptions = columnDescriptions0

    if (reader == null) {
      return new DataTable
    }

    val csvReader = new CSVReader(reader)
    val defaultFormatters = ValueFormatter.createDefaultFormatters(locale) 

    var firstLine: Boolean = true

    //todo: figure out exactly why "line" is an array!
    val lines =
      Iterator.continually(csvReader.readNext).
      takeWhile(null !=).
      filterNot(line => line.length == 1 && line(0) == "")

    var dataTable = new DataTable

    for (line <- lines) {
      if (columnDescriptions != null && line.length != columnDescriptions.size) {
        throw new CsvDataSourceException(ReasonType.INTERNAL_ERROR, "Wrong number of columns in the data.")
      }
      if (firstLine) {
        if (columnDescriptions == null) {
          columnDescriptions = Lists.newArrayList[ColumnDescription]
        }
        var tempColumnDescriptions = new ArrayList[ColumnDescription]
        for(i <- 0 to line.length) {
          val optDesc =
            Some(columnDescriptions) filter (!_.isEmpty) map (_ get i) filter (null !=)
          var id = optDesc map (_.getId) filter (null!=) getOrElse ("Col" + i)
          var colType = optDesc map (_.getType) filter (null!=) getOrElse ValueType.TEXT
          var label = optDesc map (_.getLabel) filter (null!=) getOrElse "Column" + i
          var pattern = optDesc map (_.getPattern) filter (null!=) getOrElse ""

          val tempColumnDescription = new ColumnDescription(id, colType, label)
          tempColumnDescription setPattern pattern
          tempColumnDescriptions add tempColumnDescription
        }
        if (headerRow) {
          for (i <- 0 to line.length) {
            val label = Option(line(i)) map (_.trim) getOrElse ""
            tempColumnDescriptions.get(i).setLabel(label)
          }
        }
        columnDescriptions = tempColumnDescriptions
        dataTable = new DataTable
        dataTable addColumns columnDescriptions
      }
      if (!(firstLine && headerRow)) {
        var tableRow = new TableRow
        for (i <- 0 to line.length) {
          var columnDescription = columnDescriptions.get(i)
          var valueType = columnDescription.getType
          var string = Option(line(i)) map (_.trim) orNull
          val pattern = columnDescription.getPattern
          val valueFormatter =
            if (pattern == null || pattern == "") {
              defaultFormatters.get(valueType)
            } else {
              ValueFormatter.createFromPattern(valueType, pattern, locale)
            }
          val value = valueFormatter parse string
          tableRow addCell value
        }
        try {
          dataTable.addRow(tableRow)
        } catch {
          case e: TypeMismatchException =>
        }
      }
      firstLine = false
    }
    dataTable
  }

  /**
   * Returns a Reader for the url.
   * Given a specific url, returns a Reader for that url,
   * so that the CSV data source will be able to read the CSV file from that url.
   *
   * @param url The url to get a Reader for.
   *
   * @return A Reader for the given url.
   *
   * @throws DataSourceException In case of a problem reading from the url.
   */
  def getCsvUrlReader(url: String): Reader = try {
    new BufferedReader(new InputStreamReader(new URL(url).openStream, "UTF-8"))
  } catch {
    case e: MalformedURLException => {
      throw new DataSourceException(ReasonType.INVALID_REQUEST, "url is malformed: " + url)
    }
    case e: IOException => {
      throw new DataSourceException(ReasonType.INVALID_REQUEST, "Couldn't read csv file from url: " + url)
    }
  }

  /**
   * Returns a Reader for the file.
   * Given a specific file, returns a Reader to that file,
   * so that the CSV data source will be able to read the CSV file from that file.
   *
   * @param file The file to get a Reader for.
   *
   * @return A Reader for the given file.
   *
   * @throws DataSourceException In case of a problem reading from the file.
   */
  def getCsvFileReader(file: String): Reader = try {
    new BufferedReader(new FileReader(file))
  } catch {
    case e: FileNotFoundException => {
      throw new DataSourceException(ReasonType.INVALID_REQUEST, "Couldn't read csv file from: " + file)
    }
  }

 }
