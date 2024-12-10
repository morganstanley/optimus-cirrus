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

import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.priql.RelationElementWrapper
import optimus.platform.relational.serialization.ClassEntityInfoSerialization
import optimus.platform.relational.serialization.RelationElementSerialization
import optimus.platform.temporalSurface._

import scala.jdk.CollectionConverters._

trait TemporalSurfaceMatcherSerialization extends ProtoSerializationTo {
  implicit val temporalSurfaceMatcherSerializer: TemporalSurfaceMatcherSerializer.type =
    TemporalSurfaceMatcherSerializer
}

object TemporalSurfaceMatcherSerializer
    extends ClassEntityInfoSerialization
    with RelationElementSerialization
    with NamespaceSerialization
    with ProtoSerializerTo[TemporalSurfaceMatcher, TemporalSurfaceMatcherProto] {
  /* private[this] def getTemporalSurfaceMatcherProtoForClass(
      tsm: TemporalSurfaceMatcherT): TemporalSurfaceMatcherProto.Builder = {
    require(tsm.classes.nonEmpty, "Must have at least 1 class.")
    require(tsm.namespaceWrappers.isEmpty, "Must have empty namespaceWrappers.")
    require(tsm.reactiveQueryProcessors.isEmpty, "Must have empty reactiveQueryProcessors.")
    require(tsm.relationElementWrappers.isEmpty, "Must have empty relationElementWrappers.")
    TemporalSurfaceMatcherProto.newBuilder
      .setType(TemporalSurfaceMatcherProto.Type.FOR_CLASS)
      .addAllFqns(tsm.classes.map { c: Class[_] =>
        FqnProto.newBuilder.setFqClassName(c.getName).build
      }.asJava)
  } */

  /* private[this] def getTemporalSurfaceMatcherProtoForPackage(
      tsm: TemporalSurfaceMatcherT): TemporalSurfaceMatcherProto.Builder = {
    require(tsm.classes.isEmpty, "Must have empty classes.")
    require(tsm.namespaceWrappers.nonEmpty, "Must have at least 1 package.")
    require(tsm.reactiveQueryProcessors.isEmpty, "Must have empty reactiveQueryProcessors.")
    require(tsm.relationElementWrappers.isEmpty, "Must have empty relationElementWrappers.")
    TemporalSurfaceMatcherProto.newBuilder
      .setType(TemporalSurfaceMatcherProto.Type.FOR_PACKAGE)
      .addAllNamespaces(tsm.namespaceWrappers.map(namespaceSerializer.serialize(_)).asJava)
  } */

  /* private[this] def getTemporalSurfaceMatcherProtoForPriql(
      tsm: TemporalSurfaceMatcherT): TemporalSurfaceMatcherProto.Builder = {
    require(tsm.classes.isEmpty, "Must have empty classes.")
    require(tsm.namespaceWrappers.isEmpty, "Must have empty namespaceWrappers.")
    require(tsm.reactiveQueryProcessors.nonEmpty, "Must have at least 1 reactiveQueryProcessor.")
    require(tsm.relationElementWrappers.nonEmpty, "Must have empty relationElementWrappers.")
    TemporalSurfaceMatcherProto.newBuilder
      .setType(TemporalSurfaceMatcherProto.Type.PRIQL)
      .addAllEntityInfos(tsm.reactiveQueryProcessors.map(_.entityInfo).map(toProto(_)).asJava)
      .addAllListsOfRelationElements(tsm.relationElementWrappers.map { l: Seq[RelationElementWrapper] =>
        ListOfRelationElementProto.newBuilder.addAllRelationElements(l.map(toProto(_)).asJava).build
      }.asJava)
  } */

  override def serialize(tsm: TemporalSurfaceMatcher): TemporalSurfaceMatcherProto = ??? /* {
    (tsm match {
      case DataFreeTemporalSurfaceMatchers.all =>
        TemporalSurfaceMatcherProto.newBuilder.setType(TemporalSurfaceMatcherProto.Type.ALL)
      case DataFreeTemporalSurfaceMatchers.none =>
        TemporalSurfaceMatcherProto.newBuilder.setType(TemporalSurfaceMatcherProto.Type.NONE)
      case o: TemporalSurfaceMatcherT if o.classes.nonEmpty           => getTemporalSurfaceMatcherProtoForClass(o)
      case o: TemporalSurfaceMatcherT if o.namespaceWrappers.nonEmpty => getTemporalSurfaceMatcherProtoForPackage(o)
      case o: TemporalSurfaceMatcherT if o.reactiveQueryProcessors.nonEmpty => getTemporalSurfaceMatcherProtoForPriql(o)
      case _ => throw new InvalidTemporalSurfaceMatcherException(tsm)
    }).build
  } */
}
