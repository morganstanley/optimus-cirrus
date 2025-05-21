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
package optimus.graph.tracking.handler

import optimus.platform.Tweak
import optimus.ui.ScenarioReference

sealed abstract class HandlerException(msg: String) extends Exception(msg)

final class OutOfScopeScenarioReference(
    val topScenario: ScenarioReference,
    val requestedScenario: ScenarioReference,
    extraMessage: String = ""
) extends HandlerException(
      s"Cannot refer to $requestedScenario when the consistent sub-tree root scenario is $topScenario$extraMessage"
    )

final class ImmediatelyInsideInBackground
    extends HandlerException("InBackground cannot contain Immediately. Consider using Separately instead.")

final class NoResultEvaluated extends HandlerException("NoResult() step should not be evaluated")

final class IllegalScenarioReferenceTweak(val sr: ScenarioReference, val tweaks: Seq[Tweak])
    extends Exception({
      val addendum = if (tweaks.isEmpty) "" else s":\n\t${tweaks mkString "\n\t"}"
      s"Cannot add tweaks to nontweakable ScenarioReference $sr$addendum"
    })
