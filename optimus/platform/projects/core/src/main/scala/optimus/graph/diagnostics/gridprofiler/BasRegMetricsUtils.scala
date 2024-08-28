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
package optimus.graph.diagnostics.gridprofiler

import com.opencsv.CSVReader
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.util.PrettyStringBuilder
import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer

class BasRegMetricsCmdLine {
  import org.kohsuke.args4j.Option

  @Option(name = "-i", aliases = Array("--inputFilePath"), usage = "Path to input regression CSV")
  val inputFilePath: String = ""

  @Option(name = "-o", aliases = Array("--outputFilePath"), usage = "Path to output regression CSV")
  val outputFilePath: String = ""
}

/**
 * Helper script for regression CSVs: Reads MODULE_DISPLAY_NAME, BAS_PRECOMPUTATION_TIME and
 * REG_PRECOMPUTATION_TIME and outputs new CSV with signed diff and percentage diff (negative means improvement in time
 * between bas and reg, positive means regression). Logs summary information about total number of tests that
 * regressed/improved, and total time saved/lost.
 */
object BasRegMetricsUtils extends App {
  import BasRegMetric._

  private[gridprofiler] lazy val log = getLogger(this)
  private lazy val cmdLine = new BasRegMetricsCmdLine
  private lazy val parser = new CmdLineParser(cmdLine)

  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  object BasRegMetric {
    val NAME_HEADER = "MODULE_DISPLAY_NAME"
    val BAS_HEADER = "BAS_PRECOMPUTATION_TIME"
    val REG_HEADER = "REG_PRECOMPUTATION_TIME"

    // parse hh:mm:ss to seconds for easier comparison
    private val formatter = new PeriodFormatterBuilder()
      .appendHours()
      .appendSuffix("h:")
      .appendMinutes()
      .appendSuffix("m:")
      .appendSeconds()
      .appendSuffix("s")
      .toFormatter

    def convertTimestampToLong(timestamp: String, row: Int, col: Int): Long =
      try {
        val period = formatter.parsePeriod(timestamp)
        period.toStandardDuration.getStandardSeconds
      } catch {
        case _: Exception =>
          log.warn(s"Could not parse row $row, column $col timestamp: $timestamp")
          -1
      }

    def formatString(seconds: Long): String = {
      val period = Duration.standardSeconds(seconds).toPeriod()
      val h = period.getHours
      val m = period.getMinutes
      val s = period.getSeconds
      val neg = if (h == 0 && (m < 0 || s < 0)) "-" else ""
      // only display - on the h if negative, not on m and s
      s"$neg${h}h:${m.abs}m:${s.abs}s"
    }
  }

  final case class BasRegMetric(name: String, bas: Long, reg: Long) {
    import BasRegMetric._
    def diff: Long = reg - bas
    def isRegression: Boolean = reg > bas
    def failedParsing: Boolean = bas <= 0 || reg <= 0
    def percentageChange: Double = (diff.toDouble / bas.toDouble) * 100
    def basString: String = formatString(bas)
    def regString: String = formatString(reg)
    def diffString: String = formatString(diff)
  }

  /** extracted for testing - caller should take care of opening and closing the source */
  private[gridprofiler] def readFromSource(reader: CSVReader): Seq[BasRegMetric] = {
    var nameCol = 0
    var basCol = 0
    var regCol = 0
    val data = ArrayBuffer[BasRegMetric]()
    var row = 0
    val allRows = reader.readAll().iterator()
    while (allRows.hasNext) {
      val cols = allRows.next()
      if (row == 0) {
        nameCol = cols.indexOf(NAME_HEADER)
        basCol = cols.indexOf(BAS_HEADER)
        regCol = cols.indexOf(REG_HEADER)
      } else {
        val basTime = convertTimestampToLong(cols(basCol), row, basCol)
        val regTime = convertTimestampToLong(cols(regCol), row, regCol)
        val basRegMetric = BasRegMetric(cols(nameCol), basTime, regTime)
        data += basRegMetric
      }
      row += 1
    }
    data
  }

  private def readCSV(path: String): Seq[BasRegMetric] = {
    var reader: CSVReader = null
    var data: Seq[BasRegMetric] = null
    try {
      reader = new CSVReader(new FileReader(path))
      data = readFromSource(reader)
    } catch {
      case e: Exception => log.error(s"Couldn't read input file $path", e)
    } finally reader.close()
    data
  }

  private def writeOutput(path: String, data: Seq[BasRegMetric]): Unit = {
    val str = new PrettyStringBuilder
    val header = "Name, BAS, REG, Diff, Percentage Change"
    str.append(header).endln()
    data.foreach { row =>
      str.append(s"${row.name}, ${row.basString}, ${row.regString}, ${row.diffString}, ${row.percentageChange}").endln()
    }
    val writer = new BufferedWriter(new FileWriter(path))
    try writer.write(str.toString)
    finally writer.close()
  }

  private def reportTotal(column: BasRegMetric => Long, data: Seq[BasRegMetric]): String =
    BasRegMetric.formatString(data.map(column).sum)

  /** extracted for testing */
  private[gridprofiler] def totalImproved(data: Seq[BasRegMetric]): Int = data.count(!_.isRegression)
  private[gridprofiler] def totalRegressed(data: Seq[BasRegMetric]): Int = data.count(_.isRegression)
  private[gridprofiler] def totalBAS(data: Seq[BasRegMetric]): String = reportTotal(_.bas, data)
  private[gridprofiler] def totalREG(data: Seq[BasRegMetric]): String = reportTotal(_.reg, data)
  private[gridprofiler] def totalDiff(data: Seq[BasRegMetric]): String = reportTotal(_.diff, data)

  def run(): Unit = {
    val data = readCSV(cmdLine.inputFilePath).sortBy(_.percentageChange)
    writeOutput(cmdLine.outputFilePath, data)
    val parsedData = data.filterNot(_.failedParsing)
    log.info(s"Failed to parse ${data.length - parsedData.length} rows")
    log.info(s"Successfully parsed ${parsedData.length} rows")
    log.info(s"Total improved: ${totalImproved(parsedData)}")
    log.info(s"Total regressed: ${totalRegressed(parsedData)}")
    log.info(s"Total BAS time: ${totalBAS(parsedData)}")
    log.info(s"Total REG time: ${totalREG(parsedData)}")
    log.info(s"Total diff time: ${totalDiff(parsedData)}")
  }

  run()
}
