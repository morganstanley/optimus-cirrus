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
package optimus.tools.scalacplugins.entity.reporter

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter

/**
 * Dumps metadata for all of the plugin and macro alarms in to a CSV file for analysis purposes.
 */
object AlarmCodeDumper extends App {
  val alarmHolders: Seq[OptimusAlarms] = Seq(
    OptimusErrors,
    OptimusNonErrorMessages,
    StagingErrors,
    StagingNonErrorMessages,
    PartialFunctionAlarms,
    DALAlarms,
    CopyMethodAlarms,
    MacroUtilsAlarms,
    RelationalAlarms,
    ReactiveAlarms,
    UIAlarms,
    VersioningAlarms
  )

  if (args.size != 1) {
    throw new IllegalArgumentException("Specify exactly one argument: Filename to dump alarm codes into")
  }

  val file = new File(args.head)
  file.getParentFile.mkdirs()
  val writer = new PrintWriter(new BufferedWriter(new FileWriter(file)))

  try {
    writer.println("code,severity,class,message")
    alarmHolders.foreach { holder =>
      val className = holder.getClass.getSimpleName.replace("$", "")
      holder.idToText.toSeq.sortBy(_._1.sn).foreach { case (id, text) =>
        val escapedText = """"${text.replace("\"", "\"\"")}""""
        writer.println(s"${id.sn},${id.tpe},${className},${escapedText}")
      }
    }
  } finally {
    writer.close()
  }
}
