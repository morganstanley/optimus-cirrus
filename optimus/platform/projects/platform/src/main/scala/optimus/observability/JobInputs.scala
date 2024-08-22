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
package optimus.observability

import optimus.graph.JobForwardingPluginTagKey
import optimus.graph.JobNonForwardingPluginTagKey
import optimus.graph.JobTweakExtractor
import optimus.platform._
import optimus.platform.inputs.ProcessState
import optimus.platform.inputs.loaders.FoldedNodeInput

private[observability] object JobInputs {
  def toTweakJobInputs(tweaks: Seq[Tweak]): Set[TweakJobInput] =
    // it is the caller's responsibility to ensure that tweaks have been validated via
    // JobTweakExtractor
    tweaks.map { t => TweakJobInput(t.target.propertyInfo, t.tweakValue) }.toSet

  def extractTweakJobInputs(ss: ScenarioStack): Set[TweakJobInput] = {
    // require scenario to be maximum depth 2 (enforced by ScenarioStackFilter.filter)
    assert(ss.parent.isRoot, "Scenario Stack in this @job must be of size 2")
    assert(ss.parent.topScenario.isEmpty, "No tweaks expected in root scenario stack")
    toTweakJobInputs(ss.topScenario.allTweaks)
  }

  def extractSIJobInputs(ss: ScenarioStack): Set[SIJobInput] = {
    def accumulate(set: Set[SIJobInput], f: FoldedNodeInput): Set[SIJobInput] = f.nodeInput match {
      case _: JobForwardingPluginTagKey[_] | _: JobNonForwardingPluginTagKey[_] =>
        set // do not store job transparent plugin tags
      case _ => set + SIJobInput(f)
    }
    val siInputs = ss.siParams.nodeInputs.freeze.foldLeft(Set.empty[SIJobInput])(accumulate)
    ProcessState.snapshotCurrentState.foldLeft(siInputs)(accumulate)
  }
}
