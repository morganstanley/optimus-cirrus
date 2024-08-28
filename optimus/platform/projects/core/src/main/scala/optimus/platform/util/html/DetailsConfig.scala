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
package optimus.platform.util.html

object DetailsConfig {
  // for NodeView and ValueInspector (ie, output in panels)
  // -- note, this is here and not in MaxCharUtils since DebuggerUI visible everywhere
  val maxCharsMinimum = 1000

  val default = DetailsConfig(
    result = true,
    args = true,
    entity = false,
    properties = false,
    tweakDiffs = false,
    xsTweaks = false,
    resultEntityTemporalInfo = false,
    scenarioStack = false,
    scenarioStackSmart = false,
    scenarioStackEffective = false,
    wrap = false
  )
}

final case class DetailsConfig(
    result: Boolean,
    args: Boolean,
    entity: Boolean,
    properties: Boolean,
    tweakDiffs: Boolean,
    xsTweaks: Boolean,
    resultEntityTemporalInfo: Boolean,
    scenarioStack: Boolean,
    scenarioStackSmart: Boolean,
    scenarioStackEffective: Boolean,
    wrap: Boolean,
    maxChars: Int = DetailsConfig.maxCharsMinimum)
