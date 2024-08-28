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

import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.gridprofiler.GridProfiler.log
import optimus.graph.diagnostics.gridprofiler.GridProfilerDefaults.defaultAggregationType
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.Messages
import org.kohsuke.args4j.spi.Parameters
import org.kohsuke.args4j.spi.Setter
import org.kohsuke.args4j.spi.{OptionHandler => Args4JOptionHandler}

object AggregationType extends Enumeration {

  // by task id
  val NONE = Value("NONE")

  // by engine name (former "none")
  val ENGINE = Value("ENGINE")

  // by depth of distribution (client = 0, tasks disted from the client = 1, tasks from those tasks = 2
  val DEPTH = Value("DEPTH")

  // two values: client and everything else
  val CLIENTANDREMOTE = Value("CLIENTANDREMOTE")

  // single value
  val AGGREGATED = Value("AGGREGATED")

  val DEFAULT: AggregationType.Value = CLIENTANDREMOTE

  def withNameOrDefault(name: String): AggregationType.Value = {
    if ((name eq null) || name.isEmpty) DEFAULT
    else withName(name.toUpperCase)
  }

  private[gridprofiler] def setDefault(aggregationType: AggregationType.Value): Unit = {
    defaultAggregationType = Option(aggregationType).getOrElse(AggregationType.DEFAULT)
    if (DiagnosticSettings.initialProfileAggregation != null) {
      if (defaultAggregationType != AggregationType.DEFAULT)
        log.warn("Profile aggregation mode specified both on command line and JVM option. Command line ignored!")
      defaultAggregationType =
        AggregationType.withNameOrDefault(DiagnosticSettings.initialProfileAggregation.toUpperCase)
    }
  }

  class OptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[_ >: AggregationType.Value])
      extends Args4JOptionHandler[AggregationType.Value](parser, option, setter) {
    override def parseArguments(params: Parameters): Int = {
      try {
        setter.addValue(AggregationType.withNameOrDefault(params.getParameter(0).toUpperCase))
      } catch {
        case ex: java.util.NoSuchElementException =>
          throw new CmdLineException(
            owner,
            Messages.ILLEGAL_OPERAND.format(option.toString, params.getParameter(0)),
            ex)
      }
      1
    }
    override def getDefaultMetaVariable: String =
      "[ " + (AggregationType.values map (_.toString.toLowerCase) reduceLeft (_ + " | " + _)) + " ]"
  }
}
