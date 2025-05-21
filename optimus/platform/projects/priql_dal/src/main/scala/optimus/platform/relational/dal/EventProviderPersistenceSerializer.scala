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
package optimus.platform.relational.dal

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.protobuf.ByteString
import optimus.platform.DynamicKey
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{
  Builder => RelBuilder,
  ElementType => ElemType
}
import optimus.platform.relational.persistence.protocol.{RelExpr, _}
import optimus.platform.relational.tree.{ProviderRelation, TypeInfo}

import scala.jdk.CollectionConverters._

object EventProviderPersistence extends ProviderPersistence {
  override def createRelationElementBuilder(provider: ProviderRelation, encodeType: TypeInfoEncoder): RelBuilder = {
    val providerBuilder = RelExpr.ProviderRelation.newBuilder
    providerBuilder.setProviderType(getDeserializableProviderName)

    val name = EventProviderPersistence.getClass.getName
    providerBuilder.setProviderDeserializerName(name.substring(0, name.indexOf("$")))

    val eventTypeName = provider.rowTypeInfo.runtimeClassName
    val eventProviderBuilder = ProviderSource.EventProviderRelation.newBuilder
    val eventProvider = eventProviderBuilder.setEventTypeName(eventTypeName).build
    val byteStream = new ByteArrayOutputStream()
    eventProvider.writeDelimitedTo(byteStream)

    val builder = RelExpr.RelationElement.newBuilder
    if (provider.key.fields != null) {
      builder.addAllKeys(ProviderPersistence.encodeRelationKeys(provider.key.fields).asJava)
    }

    builder
      .setNodeType(ElemType.Provider)
      .setItemType(encodeType(provider.projectedType()))
      .setProvider(providerBuilder)
      .setData(ByteString.copyFrom(byteStream.toByteArray))

  }

  override def createRelationElement(
      relation: RelExpr.RelationElement,
      decodeType: TypeInfoDecoder): ProviderRelation = {
    val source = relation.getData
    val eventProvider =
      ProviderSource.EventProviderRelation.parseDelimitedFrom(new ByteArrayInputStream(source.toByteArray))
    val eventTypeName = eventProvider.getEventTypeName

    // Type info of entity is generally unavailable.
    val typeInfo = TypeInfo.mock(eventTypeName)

    val keys = ProviderPersistence.decodeRelationKeys(relation.getKeysList.asScalaUnsafeImmutable)
    new EventStorableRelationElement[Any](typeInfo.cast[Any], DynamicKey(keys))
  }

  override def getDeserializableProviderName: String = "EventProvider"

  override def canDeserializable(relation: RelExpr.RelationElement): Boolean =
    relation.getProvider.getProviderType.equals(getDeserializableProviderName)
}
