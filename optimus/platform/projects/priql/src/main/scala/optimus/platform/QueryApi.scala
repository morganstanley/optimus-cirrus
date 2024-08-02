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
package optimus.platform

import optimus.platform._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.{KeyPropagationPolicy => KeyPolicy, _}
import optimus.platform.relational.dal.streams.EventProvider

trait QueryApi {
  def fromWithKey[T, F[_ <: T]](src: F[T], key: RelationKey[T], policy: KeyPolicy = KeyPolicy.EntityAndDimensionOnly)(
      implicit
      conv: QueryConverter[T, F],
      itemType: TypeInfo[T],
      pos: MethodPosition): Query[T] = {
    conv.convert(src, if (key eq null) NoKey else key, ProviderWithKeyPropagationPolicy(policy))(itemType, pos)
  }

  // private[optimus] while streams is still in development
  private[optimus] def events[T <: Entity](
      src: EntityCompanionBase[T])(implicit itemType: TypeInfo[T], pos: MethodPosition): Query[T] = {
    val key = KeyPolicy.NoKey.mkKey[T]
    val provider = EventProvider(src.info, itemType, key, pos)
    QueryProvider.NoKey.createQuery(provider)
  }

  // Helper overload for Scala 2.13.
  // See OPTIMUS-49610
  def from[T <: Entity](src: EntityCompanionBase[T])(implicit
      conv: QueryConverter[T, EntityCompanionBase],
      itemType: TypeInfo[T],
      pos: MethodPosition): Query[T] = from[T, EntityCompanionBase](src)(conv, itemType, pos)

  def from[T, F[_ <: T]](src: F[T], policy: KeyPolicy = KeyPolicy.EntityAndDimensionOnly)(implicit
      conv: QueryConverter[T, F],
      itemType: TypeInfo[T],
      pos: MethodPosition): Query[T] = {
    conv.convert(src, policy.mkKey[T], ProviderWithKeyPropagationPolicy(policy))(itemType, pos)
  }

  def fromTable[T, F[_ <: T]](
      src: F[T])(implicit conv: QueryConverter[T, F], itemType: TypeInfo[T], pos: MethodPosition): Query[T] = {
    conv.convert(src, NoKey, QueryProvider.NoKey)(itemType, pos)
  }
}

private class ProviderWithKeyPropagationPolicy(val policy: KeyPolicy)
    extends DefaultQueryProvider
    with ProxyKeyPropagationPolicy {}

private[platform] object ProviderWithKeyPropagationPolicy {

  def apply(policy: KeyPolicy): QueryProvider = {
    policy match {
      case KeyPolicy.EntityAndDimensionOnly => QueryProvider.EntityAndDimensionOnly
      case KeyPolicy.Legacy                 => QueryProvider.Legacy
      case KeyPolicy.NoKey                  => QueryProvider.NoKey
      case provider: QueryProvider          => provider
      case _                                => new ProviderWithKeyPropagationPolicy(policy)
    }
  }
}
