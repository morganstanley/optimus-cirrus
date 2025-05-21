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
package optimus.platform.relational.persistence.protocol

import optimus.core.utils.RuntimeMirror
import optimus.platform._
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.RelationalException
import optimus.platform.relational.inmemory.ScalaTypeMultiRelation
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{ElementType => ElemType}
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{Builder => RelBuilder}
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree._

import scala.jdk.CollectionConverters._

trait ProviderPersistenceSupport { self: ProviderRelation =>
  def serializerForThisProvider: ProviderPersistence
}

trait ProviderPersistence {

  def createRelationElementBuilder(provider: ProviderRelation, encodeType: TypeInfoEncoder): RelBuilder

  def createRelationElement(relation: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ProviderRelation

  /**
   * test whether this RelExpr.RelationElement can deserialized using this deserializer
   */
  def canDeserializable(relation: RelExpr.RelationElement): Boolean

  /**
   * the method is used for picking up the correct deserializer to a provider GPB message, it should return the same
   * value defined in ProviderRelation.getProviderName
   */
  def getDeserializableProviderName: String

}

object ProviderPersistence {
  def encodeRelationKeys(keys: Seq[String]): Seq[RelExpr.RelationKey] =
    keys map { key =>
      RelExpr.RelationKey.newBuilder().setName(key).build()
    }

  def decodeRelationKeys(keys: Seq[RelExpr.RelationKey]): Seq[String] =
    keys map { _.getName }

  def findProviderPersistenceDeserializer(name: String): ProviderPersistence = {
    try {
      val module = RuntimeMirror.moduleByName(name, getClass)
      module.asInstanceOf[ProviderPersistence]
    } catch {
      case e: Exception =>
        throw new RelationalException(
          "findProviderPersistenceDeserializer got an exception: " + e.getMessage() +
            ", the cause is possibly that you haven't imported the jar which provides the implemented ProviderPersistenceSerializer specified in protocol buffer",
          e
        )
    }
  }
}

object DefaultProviderPersistence extends ProviderPersistence {
  override def createRelationElementBuilder(provider: ProviderRelation, encodeType: TypeInfoEncoder): RelBuilder = {
    val providerBuilder = RelExpr.ProviderRelation.newBuilder
    providerBuilder.setProviderType(getDeserializableProviderName)

    val builder = RelExpr.RelationElement.newBuilder
    if (provider.key != null && provider.key.fields != null) {
      builder.addAllKeys(ProviderPersistence.encodeRelationKeys(provider.key.fields).asJava)
    }
    builder
      .setNodeType(ElemType.Provider)
      .setItemType(encodeType(provider.projectedType()))
      .setProvider(providerBuilder)
  }

  override def createRelationElement(
      relation: RelExpr.RelationElement,
      decodeType: TypeInfoDecoder): ProviderRelation = {
    val keys = ProviderPersistence.decodeRelationKeys(relation.getKeysList.asScalaUnsafeImmutable)
    val tpe = decodeType(relation.getItemType)

    ScalaTypeMultiRelation(
      List.empty[Any],
      DynamicKey(keys),
      MethodPosition.unknown,
      tpe.cast[Any],
      true,
      KeyPropagationPolicy.EntityAndDimensionOnly)
  }

  override def getDeserializableProviderName: String = "DefaultProvider"

  def canDeserializable(relation: RelExpr.RelationElement): Boolean =
    relation.getProvider.getProviderType.equals(getDeserializableProviderName)
}
