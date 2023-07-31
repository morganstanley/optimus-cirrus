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
package optimus.stratosphere.scheddel.impl

import java.time.LocalDateTime

import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.utils.CommonProcess
import optimus.stratosphere.utils.DateTimeUtils.formatDateTime
import optimus.stratosphere.utils.Text._

import scala.collection.immutable.Seq

object TaskFrequency {
  val Daily = "*"
  val WorkDay = "MON,TUE,WED,THU,FRI"
}

object WindowsTaskScheduler {
  val timeFormat = "HH:mm"

  /**
   * Schedule task like this:
   *
   * schtasks.exe /Create /RU batko /SC ONCE /TN runNotepad /TR notepad /ST 03:00 /SD 25/08/2017 /F
   */
  def scheduleTaskOnce(
      taskName: String,
      taskCommand: Seq[String],
      startDateTime: LocalDateTime,
      userName: String,
      scheduleFrequency: String,
      stratoWorkspace: StratoWorkspaceCommon): Unit = {

    val dateFormat = getDateFormat(stratoWorkspace)
    val cmd = formatScheduleTaskOnceCmd(taskName, taskCommand, startDateTime, userName, scheduleFrequency, dateFormat)
    stratoWorkspace.log.info(s"Scheduling Windows task: $taskName with:")
    stratoWorkspace.log.info(s"`${cmd.mkString(" ")}`")
    new CommonProcess(stratoWorkspace).runAndWaitFor(cmd)
  }

  private[scheddel] def formatScheduleTaskOnceCmd(
      taskName: String,
      taskCommand: Seq[String],
      startDateTime: LocalDateTime,
      userName: String,
      scheduleFrequency: String,
      dateFormat: String): Seq[String] = {
    val startTime = formatDateTime(timeFormat, startDateTime)
    val startDate = formatDateTime(dateFormat, startDateTime)
    Seq(
      "schtasks.exe",
      "/Create",
      "/RU",
      doubleQuote(userName),
      "/SC",
      "WEEKLY",
      "/D",
      scheduleFrequency,
      "/TN",
      doubleQuote(taskName),
      "/TR",
      doubleQuote(taskCommand.mkString(" ")),
      "/ST",
      startTime,
      "/SD",
      startDate,
      "/F"
    )
  }

  /**
   * Adjust `dataTimeFormat`:
   *   - to hold at least two digits for each of month, day and year.
   *   - trims trailing separator if present
   *
   * E.g. transforms `M/d/yyyy/` into `MM/dd/yyyy`
   */
  private def getDateFormat(stratoWorkspace: StratoWorkspaceCommon): String = {

    // Specifies the format used to display dates in short date (numeric) format, such as 6/5/69.
    val dateTimeFormat = getFromSystemLocaleRegistry(stratoWorkspace, "sShortDate")
    // Specifies the symbol separating numbers when the date is displayed in short date (numeric) format, such as 6/5/69
    val separator = getFromSystemLocaleRegistry(stratoWorkspace, "sDate")
    val validSeparator = if (separator == "REG_SZ") "" else separator

    adjustDateTimeFormat(dateTimeFormat, validSeparator)
  }

  private[impl] def adjustDateTimeFormat(dateTimeFormat: String, separator: String): String = {
    def validSeparator(separator: String): String = {
      val defaultSeparator = "/"
      val unsupportedSeparators = Set("", "\t")
      if (unsupportedSeparators.contains(separator))
        defaultSeparator
      else
        separator
    }

    def subPattern(symbol: Char): String = {
      val numOfChars = if (symbol == 'y') 4 else 2
      List.fill(numOfChars)(symbol).mkString("")
    }

    val elementsOrder = dateTimeFormat
      .replace(separator, "")
      .toCharArray
      .distinct

    val dateSeparator = validSeparator(separator)
    elementsOrder.map(subPattern).mkString(dateSeparator)
  }

  private def getFromSystemLocaleRegistry(stratoWorkspace: StratoWorkspaceCommon, key: String): String = {
    val queryRegistry = Seq("reg", "query", "HKCU\\Control Panel\\International", "/v", key)
    val result = new CommonProcess(stratoWorkspace).runAndWaitFor(queryRegistry)

    result.split("\n").find(_.contains(key)).map(a => a.split("\\s+")) match {
      case Some(columns) => columns.last
      case None          => throw new StratosphereException("[ERROR] $key not defined in the registry.")
    }
  }
}
