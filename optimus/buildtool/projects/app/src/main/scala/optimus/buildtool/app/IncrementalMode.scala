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
package optimus.buildtool.app

import optimus.buildtool.compilers.zinc.ZincIncrementalMode
import optimus.platform._

object IncrementalMode {
  val None = IncrementalMode(ZincIncrementalMode.None, defaultUseIncrementalArtifacts = false)
}

@entity class IncrementalMode(
    val defaultZincIncrementalMode: ZincIncrementalMode,
    val defaultUseIncrementalArtifacts: Boolean
) {
  @node(tweak = true) val zincIncrementalMode: ZincIncrementalMode = defaultZincIncrementalMode
  // if false, even artifacts with perfect cache hits won't be used if they
  // were created from an incremental compilation
  @node(tweak = true) val useIncrementalArtifacts: Boolean = defaultUseIncrementalArtifacts
}
