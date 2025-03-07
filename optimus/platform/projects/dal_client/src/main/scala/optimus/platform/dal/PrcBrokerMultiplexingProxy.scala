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
package optimus.platform.dal

import optimus.platform._
import optimus.platform.dsi.bitemporal.DalPrcRedirectionResult
import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.Result

import scala.collection.mutable

// Splits read command execution between read broker and PRC server
abstract class PrcBrokerMultiplexingProxy { self: ClientSideDSI =>
  protected def replica: ClientSideDSI
  protected def prcProxyOpt: Option[PrcRemoteDsiProxy]

  private lazy val prcResultTracer = PrcResultTracerState.getPrcResultTracer

  @async protected def executePrcReadOnlyCommands(
      prcProxy: PrcRemoteDsiProxy,
      prcCmds: Seq[ReadOnlyCommand]): Seq[Result] = {
    prcResultTracer.onPrcExecution(prcCmds.size)
    prcProxy.executeReadOnlyCommands(prcCmds)
  }

  @async protected def executeReplicaReadOnlyCommands(replicaCmds: Seq[ReadOnlyCommand]): Seq[Result] = {
    replica.executeReadOnlyCommands(replicaCmds)
  }

  @async override def executeReadOnlyCommands(cmds: Seq[ReadOnlyCommand]): Seq[Result] = {
    prcProxyOpt
      .map { prcProxy =>
        val unfinishedCmds = mutable.ListMap.empty[Int, ReadOnlyCommand] ++
          cmds.zipWithIndex.map { case (cmd, idx) => idx -> cmd }
        val finishedCmds = mutable.Map.empty[Int, Result]

        def finishCmd(idx: Int, result: Result): Unit = {
          require(unfinishedCmds.contains(idx) && !finishedCmds.contains(idx))
          unfinishedCmds -= idx
          finishedCmds += (idx -> result)
        }

        val (prcIdxs, prcCmds) = unfinishedCmds.filter { case (_, cmd) => prcProxy.eligibleForPrc(cmd) }.toSeq.unzip
        if (prcCmds.nonEmpty) {
          val prcResults = executePrcReadOnlyCommands(prcProxy, prcCmds).zip(prcIdxs)
          prcResults.foreach {
            case (redirection: DalPrcRedirectionResult, _) => prcResultTracer.onRedirection(redirection)
            case (result, idx)                             => finishCmd(idx, result)
          }
        }

        val (replicaIdxs, replicaCmds) = unfinishedCmds.unzip
        if (replicaCmds.nonEmpty) {
          val brokerResults = executeReplicaReadOnlyCommands(replicaCmds.toSeq).zip(replicaIdxs)
          brokerResults.foreach { case (res, idx) =>
            finishCmd(idx, res)
          }
        }

        require(
          finishedCmds.size == cmds.size,
          s"Some commands were not executed: ${unfinishedCmds.mkString("[", ",", "]")}")
        finishedCmds.toSeq.sortBy { case (idx, _) => idx }.map { case (_, cmd) => cmd }
      }
      .getOrElse(executeReplicaReadOnlyCommands(cmds))
  }
}
