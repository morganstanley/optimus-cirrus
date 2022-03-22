/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.utils

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import optimus.platform.IO.usingQuietly

import scala.collection.JavaConverters._
import scala.util.Try

object FileUtils {

  def fileToMapTwoColumns(filename: String): Map[String, String] = {
    Try {
      val reader = getCSVReader(filename, '\t')

      usingQuietly(reader) { r =>
        r.readAll().asScala.toArray
      }.map {
        case Array(k, v) => (k, v)
      }.toMap
    }.getOrElse(Map.empty)
  }

  def writeToFileTwoColumns(filename: String, data: Seq[Array[String]]) {
    val writer = getCSVWriter(filename, '\t')

    usingQuietly(writer) { w =>
      w.writeAll(data.asJava)
    }
  }

  def getCSVReader(filename: String, separator: Char): CSVReader = {
    new CSVReader(new BufferedReader(new InputStreamReader(new FileInputStream(filename))), separator, '"', '\\')
  }

  def getCSVWriter(filename: String, separator: Char, append: Boolean = false): CSVWriter = {
    new CSVWriter(new OutputStreamWriter(new FileOutputStream(filename, append)), separator, '"', '\\')
  }

}
