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
package optimus.platform.dal.client

import optimus.platform.dal.prc.NormalizedCacheableQuery
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.HasQuery
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTemporality
import optimus.platform.dsi.prc.cache.NonTemporalPrcKey

private[optimus] class PrcKeyProvider(baseContext: Context) {
  def mkKey(cmd: Command): Option[(NonTemporalPrcKey, ReadOnlyCommandWithTemporality with HasQuery)] =
    cmd match {
      case rwt: ReadOnlyCommandWithTemporality with HasQuery =>
        NormalizedCacheableQuery.normalize(rwt.query).map(q => NonTemporalPrcKey(q, baseContext) -> rwt)
      case _: Command =>
        // This cmd is not eligible for PRC
        None
    }
}
