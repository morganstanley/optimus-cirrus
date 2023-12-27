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
package optimus.buildtool.builders.reporter.codeanalysis

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.config.ModuleId
import scala.collection.immutable.Seq

abstract class Analyzer[T](msgs: Seq[MessagesArtifact], predicate: CompilationMessage => Boolean) {
  def groupByModule: Map[ModuleId, Seq[MessagesArtifact]] = msgs.groupBy(_.id.scopeId.fullModule)

  def generateModuleReports: Map[ModuleId, Seq[T]] = doPerModule(groupByModule)

  def doPerModule(perModuleMsgArtifacts: Map[ModuleId, Seq[MessagesArtifact]]): Map[ModuleId, Seq[T]] = {
    perModuleMsgArtifacts.map { case (module, msgArtifacts) =>
      val alerts = msgArtifacts.flatMap(_.messages).filter(predicate)
      module -> generateData(alerts, module)
    }
  }

  def generateData(alerts: Seq[CompilationMessage], moduleId: ModuleId): Seq[T]
}

final case class AlertCountAnalysis(msgs: Seq[MessagesArtifact], predicate: CompilationMessage => Boolean)
    extends Analyzer[AlertCount](msgs, predicate) {

  override def generateData(alerts: Seq[CompilationMessage], moduleId: ModuleId): Seq[AlertCount] =
    AlertCountAnalysis.toAlertCounts(alerts, moduleId)
}

object AlertCountAnalysis {
  def toAlertCounts(alerts: Seq[CompilationMessage], moduleId: ModuleId): Seq[AlertCount] =
    alerts.groupBy(_.alarmId).collect { case (id, msgs) => AlertCount(id, msgs.size) }.toIndexedSeq
}

final case class AlertCount(alarmId: Option[String], count: Int)
final case class AlertLoc(alarmId: Option[String], loc: MessagePosition, module: ModuleId)
