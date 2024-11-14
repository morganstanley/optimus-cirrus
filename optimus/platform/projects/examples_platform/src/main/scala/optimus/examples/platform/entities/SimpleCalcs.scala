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
package optimus.examples.platform.entities

import optimus.platform._

/**
 * An entity with some simple calculations, so we can demonstrate tweaks.
 *
 * Shows:
 *   - val properties defined inline in primary constructor (with default values)
 */
@entity
class SimpleCalcs(val name: String = "anonymous", @node(tweak = true) val x: Int = 7, @node val y: Int = 0) {

  @node(tweak = true) def a: Int = x * 2 + y
  @node(tweak = true) def b(factor: Double): Double = a * factor
}
