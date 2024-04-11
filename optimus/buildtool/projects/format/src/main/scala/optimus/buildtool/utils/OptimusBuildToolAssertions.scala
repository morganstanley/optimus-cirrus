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

import java.{lang => jl}

object OptimusBuildToolAssertions {
  final val Property = "optimus.buildtool.asserts"
  val enabled: Boolean = jl.Boolean.getBoolean(Property)
  // not putting the one-arg versions here to encourage explanation
  // OPTIMUS-37666 these need to be by-name in both arguments (cond can be expensive)
  def assert(cond: => Boolean, msg: => String): Unit = { if (enabled) Predef.assert(cond, msg) }
  def require(cond: => Boolean, msg: => String): Unit = { if (enabled) Predef.require(cond, msg) }
}
