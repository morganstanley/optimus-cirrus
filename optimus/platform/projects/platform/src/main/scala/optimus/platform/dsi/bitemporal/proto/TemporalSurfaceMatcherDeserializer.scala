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
package optimus.platform.dsi.bitemporal.proto

import optimus.entity.EntityInfoRegistry
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.priql.NamespaceWrapper
import optimus.platform.priql.RelationElementWrapper
import optimus.platform.relational.dal.persistence.DalRelTreeDecoder
import optimus.platform.relational.from
import optimus.platform.relational.namespace.Namespace
import optimus.platform.relational.persistence.RelationTreeMessage
import optimus.platform.relational.persistence.protocol.RelTreeDecoder
import optimus.platform.relational.reactive.ReactiveQueryProcessor
import optimus.platform.relational.serialization.ClassEntityInfoSerialization
import optimus.platform.relational.serialization.RelationElementSerialization
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.temporalSurface._
import scala.jdk.CollectionConverters._

trait TemporalSurfaceMatcherDeserialization extends ProtoSerializationFrom {
  implicit val temporalSurfaceMatcherDeserializer: TemporalSurfaceMatcherDeserializer.type =
    TemporalSurfaceMatcherDeserializer
}

object TemporalSurfaceMatcherDeserializer
    extends RelationElementSerialization
    with ClassEntityInfoSerialization
    with NamespaceSerialization
    with ProtoSerializerFrom[TemporalSurfaceMatcher, TemporalSurfaceMatcherProto] {
  private[this] def getEcb(fqn: String): EntityCompanionBase[_] = {
    EntityInfoRegistry.getCompanion(fqn).asInstanceOf[EntityCompanionBase[_]]
  }

  private[this] def allOrNoneMatcherRequirements(proto: TemporalSurfaceMatcherProto): Unit = ??? /* {
    require(proto.getFqnsCount == 0, s"Expected 0 FQNs for ALL matcher but got ${proto.getFqnsList}")
    require(proto.getNamespacesCount == 0, s"Expected 0 Namespaces for ALL matcher but got ${proto.getNamespacesList}")
    require(
      proto.getEntityInfosCount == 0,
      s"Expected 0 EntityInfos for FOR_CLASS matcher but got ${proto.getEntityInfosList}")
    require(
      proto.getListsOfRelationElementsCount == 0,
      s"Expected 0 ListOfRelationalElements for FOR_CLASS matcher but got ${proto.getListsOfRelationElementsList}")
  } */

  private[this] def getAllTemporalSurfaceMatcher(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = {
    allOrNoneMatcherRequirements(proto)
    TemporalSurfaceMatchers.all
  }

  private[this] def getNoneTemporalSurfaceMatcher(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = {
    allOrNoneMatcherRequirements(proto)
    TemporalSurfaceMatchers.none
  }

  private[this] def getForClassTemporalSurfaceMatcher(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = ??? /* {
    require(
      proto.getNamespacesCount == 0,
      s"Expected 0 Namespaces for FOR_CLASS matcher but got ${proto.getNamespacesList}")
    require(
      proto.getEntityInfosCount == 0,
      s"Expected 0 EntityInfos for FOR_CLASS matcher but got ${proto.getEntityInfosList}")
    require(
      proto.getListsOfRelationElementsCount == 0,
      s"Expected 0 ListOfRelationalElements for FOR_CLASS matcher but got ${proto.getListsOfRelationElementsList}")
    TemporalSurfaceMatchers.forClass(
      proto.getFqnsList.asScala
        .map { fqn: FqnProto =>
          getEcb(fqn.getFqClassName)
        }
        .filter(_ ne null): _*)
  } */

  private[this] def getForPackageTemporalSurfaceMatcher(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = ??? /* {
    require(proto.getFqnsCount == 0, s"Expected 0 FQNs for FOR_PACKAGE matcher but got ${proto.getFqnsList}")
    require(
      proto.getEntityInfosCount == 0,
      s"Expected 0 EntityInfos for FOR_CLASS matcher but got ${proto.getEntityInfosList}")
    require(
      proto.getListsOfRelationElementsCount == 0,
      s"Expected 0 ListOfRelationalElements for FOR_CLASS matcher but got ${proto.getListsOfRelationElementsList}")
    TemporalSurfaceMatchers.forQuery(proto.getNamespacesList.asScala.map(fromProto(_)).map { n: NamespaceWrapper =>
      from(Namespace(n.namespace, n.includesSubPackage))
    }: _*)
  } */

  private[this] def getForPriqlTemporalSurfaceMatcher(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = ??? /* {
    require(proto.getFqnsCount == 0, s"Expected 0 FQNs for ALL matcher but got ${proto.getFqnsList}")
    require(proto.getNamespacesCount == 0, s"Expected 0 Namespaces for ALL matcher but got ${proto.getNamespacesList}")
    require(
      proto.getEntityInfosCount == proto.getListsOfRelationElementsCount,
      s"Expected EntityInfos.size == ListsOfRelationElements.size but got ${proto.getEntityInfosCount} != ${proto.getListsOfRelationElementsCount}"
    )
    val decoder: RelTreeDecoder = new DalRelTreeDecoder()
    val x: Seq[ReactiveQueryProcessor] =
      proto.getEntityInfosList.asScala.zip(proto.getListsOfRelationElementsList.asScala).map {
        eiprotoAndLproto: (ClassEntityInfoProto, ListOfRelationElementProto) =>
          val (eiProto, lProto) = eiprotoAndLproto
          val res: List[RelationElementWrapper] = lProto.getRelationElementsList.asScala.map(fromProto(_)).toList
          new ReactiveQueryProcessor(
            fromProto(eiProto),
            res.map { sre: RelationElementWrapper =>
              decoder.decode(RelationTreeMessage(sre.a))
            })
      }
    QueryBasedTemporalSurfaceMatchers.buildFromReactiveQueryProcessors(x)
  } */

  override def deserialize(proto: TemporalSurfaceMatcherProto): TemporalSurfaceMatcher = ??? /* {
    proto.getType match {
      case TemporalSurfaceMatcherProto.Type.ALL         => getAllTemporalSurfaceMatcher(proto)
      case TemporalSurfaceMatcherProto.Type.NONE        => getNoneTemporalSurfaceMatcher(proto)
      case TemporalSurfaceMatcherProto.Type.FOR_CLASS   => getForClassTemporalSurfaceMatcher(proto)
      case TemporalSurfaceMatcherProto.Type.FOR_PACKAGE => getForPackageTemporalSurfaceMatcher(proto)
      case TemporalSurfaceMatcherProto.Type.PRIQL       => getForPriqlTemporalSurfaceMatcher(proto)
      case _                                            => throw new InvalidTemporalSurfaceMatcherException(proto)
    }
  } */
}
