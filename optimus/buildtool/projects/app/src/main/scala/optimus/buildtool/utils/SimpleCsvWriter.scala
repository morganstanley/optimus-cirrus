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
package optimus.buildtool.utils

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import com.opencsv.CSVWriter
import optimus.platform.util.Log
import optimus.platform._

/**
 * a base trait for simple CSV writers
 */
trait SimpleCsvWriter[T] extends Log {
  protected def defaultFilename: String
  protected def fieldExtractors: Seq[(String, T => Any)]

  final def writeCsvFile(file: File, rows: Iterable[T]): Unit = {
    val (fieldNames, extractors) = fieldExtractors.unzip
    val (ns, _) = AdvancedUtils.timed {
      val writer = new CSVWriter(new BufferedWriter(new FileWriter(file), 256 * 1024))
      try {
        writer.writeNext(fieldNames.toArray)
        rows.foreach { r =>
          writer.writeNext(extractors.map(e => format(e(r))).toArray)
        }
      } finally writer.close()
    }
    log.debug(f"Wrote ${rows.size}%,d row(s) to ${file} in ${nanosToMillis(ns)}%,dms")
  }

  final def writeCsvToDir(dir: File, rows: Iterable[T]): Unit = writeCsvFile(new File(dir, defaultFilename), rows)

  private def format(a: Any): String = a match {
    case d: Double => f"$d%.2f"
    case None      => ""
    case Some(t)   => format(t)
    case x         => x.toString
  }

  private def nanosToMillis(ns: Long): Int = (ns / 1e6).toInt
}
