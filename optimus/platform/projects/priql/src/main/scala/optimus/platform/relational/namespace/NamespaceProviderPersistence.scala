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
package optimus.platform.relational.namespace

import optimus.platform.relational.persistence.protocol._
import optimus.platform.relational.persistence.protocol.RelExpr
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.MethodPosition
import optimus.platform._
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{Builder => RelBuilder}
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{ElementType => ElemType}

import scala.jdk.CollectionConverters._
import optimus.platform.relational.provider.namespace.ProviderSource

object NamespaceProviderPersistence extends ProviderPersistence {
  override def createRelationElementBuilder(provider: ProviderRelation, encodeType: TypeInfoEncoder): RelBuilder = {
    val namespaceRelation = provider.asInstanceOf[NamespaceMultiRelation[_]]
    val providerBuilder = RelExpr.ProviderRelation.newBuilder
    providerBuilder.setProviderType(getDeserializableProviderName)

    val name = NamespaceProviderPersistence.getClass.getName
    providerBuilder.setProviderDeserializerName(name.substring(0, name.indexOf("$")))

    val namespaceProvider = ProviderSource.NamespaceProviderRelation.newBuilder
      .setTypeName(namespaceRelation.typeInfo.getClass.getName)
      .setNamespace(namespaceRelation.source.namespace)
      .setIncludesSubPackage(namespaceRelation.source.includesSubPackage)
      .build

    val builder = RelExpr.RelationElement.newBuilder
    if (namespaceRelation.key.fields != null) {
      builder.addAllKeys(ProviderPersistence.encodeRelationKeys(namespaceRelation.key.fields).asJava)
    }

    builder
      .setNodeType(ElemType.Provider)
      .setItemType(encodeType(TypeInfo.ANY))
      .setProvider(providerBuilder)
      .setData(namespaceProvider.toByteString)
  }

  override def createRelationElement(
      relation: RelExpr.RelationElement,
      decodeType: TypeInfoDecoder): ProviderRelation = {
    val namespaceProvider = ProviderSource.NamespaceProviderRelation.parseFrom(relation.getData)

    val keys = ProviderPersistence.decodeRelationKeys(relation.getKeysList.asScalaUnsafeImmutable)

    val namespace =
      if (namespaceProvider.hasIncludesSubPackage()) {
        // New mode namespace
        Namespace(namespaceProvider.getNamespace, namespaceProvider.getIncludesSubPackage)
      } else {
        // Legacy namespace which use "_" for current package and "__" for current and subpackage.
        val ns = namespaceProvider.getNamespace
        if (ns.endsWith("._"))
          Namespace(ns.substring(0, ns.length - 2), false)
        else if (ns.endsWith(".__"))
          Namespace(ns.substring(0, ns.length - 3), true)
        else
          Namespace(ns, false)
      }

    new NamespaceMultiRelation[Any](namespace, TypeInfo.ANY, DynamicKey(keys), MethodPosition.unknown)
  }

  override def getDeserializableProviderName: String = "NamespaceProvider"

  override def canDeserializable(relation: RelExpr.RelationElement): Boolean =
    relation.getProvider.getProviderType.equals(getDeserializableProviderName)
}
