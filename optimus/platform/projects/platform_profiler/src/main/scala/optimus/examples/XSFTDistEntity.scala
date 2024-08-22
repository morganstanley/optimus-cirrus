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
package optimus.examples

import optimus.graph.cache.PerPropertyCache
import optimus.platform._

@entity class XSFTDistEntity(x: Int) {
  @node(tweak = true) val tweakableA: Int = 0
  @node(tweak = true) val tweakableB: Int = 0
  @node(tweak = true) val tweakableC: Int = 0

  /** calc depends on tweakableA and tweakableC but not tweakableB */
  @node def calc(i: Int): Int = tweakableA + i + x + tweakableC
}

object XSFTDistEntity {
  calc.setCustomCache(new PerPropertyCache(100))
}
