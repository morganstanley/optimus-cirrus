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
package optimus.platform.dsi.prc.cache

import com.ms.silverking.cloud.dht.common.SimpleKey
import optimus.dal.silverking.base.DHTKeyUtils
import optimus.platform.ImmutableArray
import optimus.platform.dal.prc.DalPrcKeyT
import optimus.platform.dal.prc.NormalizedCacheableQuery
import optimus.platform.dal.prc.NormalizedPrcKeyComponent
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.Query
import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.proto.Prc.NonTemporalPrcKeyProto

final case class NonTemporalPrcKeyUserOpts(key: NonTemporalPrcKey, command: ReadOnlyCommand)

private[optimus] final case class NonTemporalPrcKey(
    normalizedKeyComponent: NormalizedPrcKeyComponent,
    context: Context,
    version: Int)
    extends DalPrcKeyT[ImmutableArray[Byte]] {
  override def getBackendKeyField: ImmutableArray[Byte] =
    ImmutableArray.wrapped(NonTemporalPrcKeySerializer.serialize(this).toByteArray)
  def cacheableQueryOpt: Option[NormalizedCacheableQuery] = Option(normalizedKeyComponent) collect {
    case q: NormalizedCacheableQuery => q
  }

  def toDhtKey: SimpleKey = {
    val key = DHTKeyUtils.createKey(this.getBackendKeyField.rawArray)
    SimpleKey.of(key)
  }
}

private[optimus] object NonTemporalPrcKey {
  def apply(normalizedQuery: NormalizedCacheableQuery, context: Context): NonTemporalPrcKey =
    ??? // apply(normalizedQuery, context, NonTemporalPrcKeyProto.Version.NORMALIZED_QUERY_PROTO.getNumber)

  def apply(query: Query, context: Context, version: Int): NonTemporalPrcKey =
    apply(NormalizedCacheableQuery(query), context, version)

  def apply(normalizedPrcKeyComponent: NormalizedPrcKeyComponent, context: Context): NonTemporalPrcKey =
    ??? // apply(normalizedPrcKeyComponent, context, NonTemporalPrcKeyProto.Version.NORMALIZED_QUERY_PROTO.getNumber)

  def apply(query: Query, context: Context): NonTemporalPrcKey =
    ??? // apply(query, context, NonTemporalPrcKeyProto.Version.NORMALIZED_QUERY_PROTO.getNumber)

}
