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
package optimus.platform.temporalSurface.advanced

import optimus.platform._
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.metadatas.internal.ClassMetaData
import optimus.platform.storable._
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.impl._
import optimus.platform.temporalSurface.operations._
import optimus.platform.util.HierarchyManager.entityHierarchyManager

@entity
object TemporalContextUtils {
  /*
    Return exact vt/tt used to load a DAL entity under a given TemporalContext/TemporalSurface
    This API does not access DAL under any situation. See TestLoadCoordinatesFromTemporalContext for examples
    When the second parameter is None,
    we use dal$temporalContext of entity to query the temporal information of linkage entity access
   */
  @scenarioIndependent @node def loadTemporalCoordinates(
      e: Entity,
      tc: Option[TemporalContext] = None): TemporalQueryResult = {
    e.dal$universe match {
      case DALUniverse =>
        val tcActual = tc.getOrElse(e.dal$temporalContext)
        tcActual match {
          case tcImpl: TemporalContextImpl =>
            val dataAccess = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl].dataAccess
            val query = QueryByEntityReference(
              dataAccess,
              EntityReferenceQuery(e.dal$entityRef, e.getClass, true, EntityReferenceQueryReason.Internal))
            tcImpl.findExactTemporality(query)
        }
      case AppliedUniverse => AppliedEntity
      case UniqueUniverse  => UniqueEntity
    }
  }

  // TODO (OPTIMUS-15608) Support Priql Based TemporalSurfaceMatcher to match the value of entity not just the class of entity
  @scenarioIndependent @node def loadTemporalCoordinatesForEntityClass[E <: Entity](
      cls: Class[E],
      tc: TemporalContext): TemporalQueryResult = {
    tc match {
      case tcImpl: TemporalContextImpl =>
        val dataAccess = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl].dataAccess
        val query = QueryByClass(dataAccess, cls)
        tcImpl.findExactTemporality(query)
    }
  }
  /*
   * Return a Map of className -> all storable Entity implementations for the specified className.
   * If onlyConcrete is true, Only storableConcreteEntity will be returned
   */
  def getAllStorableEntityImplsForClasses(classNames: Set[String], onlyConcrete: Boolean): Map[String, Set[String]] = {
    classNames.map(className => (className -> getAllStorableEntityImplsForClass(className, onlyConcrete))).toMap
  }

  /*
   * Return all storable Entity implementations for the specified className. If onlyConcrete is true, Only storableConcreteEntity will be returned
   */
  def getAllStorableEntityImplsForClass(className: String, onlyConcrete: Boolean): Set[String] =
    getAllChildrenForClass(className, true, true, true, onlyConcrete, true)

  def getAllChildrenForClass(
      className: String,
      includeSelf: Boolean,
      onlyEntity: Boolean,
      onlyStorable: Boolean,
      onlyConcrete: Boolean,
      isFullClassName: Boolean): Set[String] = {
    entityHierarchyManager.metaData
      .get(className)
      .map { cls =>
        val result = Function.chain[Set[ClassMetaData]](Seq(
          { x => if (includeSelf) x + cls else x },
          { x => if (onlyEntity) x.filter(_.isEntity) else x },
          { x => if (onlyStorable) x.filter(_.isStorable) else x },
          { x => if (onlyConcrete) x.filter(c => !c.isAbstract && !c.isTrait) else x }
        ))(cls.allChildren)
        if (isFullClassName) result.map(_.fullClassName) else result.map(_.localClassName)
      }
      .getOrElse(Set.empty[String])
  }

}
