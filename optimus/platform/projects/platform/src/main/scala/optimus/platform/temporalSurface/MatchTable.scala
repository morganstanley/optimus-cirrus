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
package optimus.platform.temporalSurface

import java.io.ObjectStreamException
import java.io.Serializable
import java.{util => ju}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import msjava.slf4jutils.scalalog.getLogger
import optimus.datatype.DatatypeUtil
import optimus.graph.Settings
import optimus.platform.entity
import optimus.platform.metadatas.internal.{ClassMetaData, EntityBaseMetaData}
import optimus.platform.node
import optimus.platform.util.{ClientEntityHierarchy, EntityHierarchyManager}
import optimus.platform.storable.Entity
import optimus.platform.temporalSurface.operations._
import optimus.platform._
import optimus.platform.metadatas.internal.PIIDetails

import scala.collection.mutable

object SerializableMatchTable {
  private val log = getLogger(this)
  // note this is not thread safe. testing only...
  private[optimus /*platform*/ ] var missListener: SerializableMatchTable => Unit = _
  private[optimus /*platform*/ ] def clearCache(): Unit = { matchTableCache.invalidateAll() }

  private val matchTableCache: LoadingCache[SerializableMatchTable, MatchTable] =
    CacheBuilder
      .newBuilder()
      .weakValues()
      .maximumSize(Settings.matchTableDeserializeCacheSize)
      .build(CacheLoader.from((smt: SerializableMatchTable) => {
        if (missListener ne null) missListener(smt)
        MatchTable.buildImpl(smt.namespaces, smt.classNames)
      }))
}

final case class SerializableMatchTable(classNames: Set[String], namespaces: Set[(String, Boolean)]) {
  import SerializableMatchTable._

  @throws(classOf[java.io.ObjectStreamException])
  def readResolve(): AnyRef = {
    log.debug(s"deserialized match table: ${hashCode()} for: classNames: $classNames, namespaces: $namespaces")
    matchTableCache.get(this)
  }
}

private[optimus] final class MatchTable private (
    private val classNames: Set[String],
    private val namespaces: Set[(String, Boolean)],
    private val hierarchy: EntityHierarchyManager,
    private[optimus] val alwaysMatchClasses: Set[String],
    private[optimus] val cantTellClasses: Set[String])
    extends Serializable {

  private[this] val classLookup: ju.HashMap[String, ClassMatchResult] = {
    val cl = new ju.HashMap[String, ClassMatchResult]()
    alwaysMatchClasses.foreach(cl.put(_, AlwaysMatch))
    cantTellClasses.foreach(cl.put(_, CantTell))
    cl
  }

  private[optimus] def matchesNamespace(ns: String): Boolean = {
    namespaces.exists(n => n._1 == ns || (n._2 && ns.startsWith(n._1 + "."))) ||
    alwaysMatchClasses.exists(isClassInNamespace(_, ns)) ||
    cantTellClasses.exists(isClassInNamespace(_, ns))
  }

  private def isClassInNamespace(clazz: String, namespace: String) = {
    clazz.startsWith(namespace) && clazz.length > namespace.length && clazz.lastIndexOf('.') == namespace.length
  }

  def tryMatch(clz: Class[_]): ClassMatchResult = {
    val result = classLookup.get(clz.getName)
    if (result ne null) result else NeverMatch
  }

  override def toString() = s"MatchTable(alwaysMatch = $alwaysMatchClasses, cantTell = $cantTellClasses)"

  override def hashCode(): Int = alwaysMatchClasses.hashCode() * 31 + cantTellClasses.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case that: MatchTable =>
      if (this eq that) true
      else this.alwaysMatchClasses == that.alwaysMatchClasses && this.cantTellClasses == that.cantTellClasses
    case other => false
  }

  @throws(classOf[ObjectStreamException])
  def writeReplace(): AnyRef = {
    assert(
      hierarchy eq MatchTable.defaultEntityHierachyManager,
      "only default entity hierarchy manager can be serialized")
    SerializableMatchTable(classNames, namespaces)
  }
}

@entity private[optimus] object MatchTable {

  private val entityClassMetaData = {

    val piiElementList: Seq[PIIDetails] = DatatypeUtil.extractPiiElementsList(classOf[Entity].getDeclaredFields)

    val entityBaseMetaData = EntityBaseMetaData(
      fullClassName = classOf[Entity].getName,
      packageName = classOf[Entity].getPackage.getName,
      isStorable = true,
      isAbstract = true,
      isObject = false,
      isTrait = true,
      slotNumber = 0,
      explicitSlotNumber = false,
      parentNames = Nil,
      piiElements = piiElementList.toSeq
    )
    ClassMetaData(entityBaseMetaData)
  }
  val defaultEntityHierachyManager = ClientEntityHierarchy.hierarchy

  private def createMatchTable(
      namespaces: Set[(String, Boolean)],
      classNames: Set[String],
      hierarchy: EntityHierarchyManager,
      alwaysMatch: Set[ClassMetaData],
      cantTell: Set[ClassMetaData]) = {
    val alwaysMatchClasses = alwaysMatch.filter(_.isStorable).map(_.fullClassName)
    val cantTellClasses = cantTell.filter(_.isStorable).map(_.fullClassName)
    new MatchTable(classNames, namespaces, hierarchy, alwaysMatchClasses, cantTellClasses)
  }

  private def inOneNamespace(meta: ClassMetaData, namespaces: Set[(String, Boolean)]): Boolean = {
    namespaces.exists { case (pkg, includeSub) =>
      if (includeSub) meta.fullClassName.startsWith(pkg + ".")
      else meta.packageName == pkg
    }
  }

  @entersGraph private[optimus] def build(
      namespaces: Set[(String, Boolean)],
      classNames: Set[String],
      hierarchy: EntityHierarchyManager = defaultEntityHierachyManager): MatchTable =
    buildCached(namespaces, classNames, hierarchy)

  @scenarioIndependent @node private def buildCached(
      namespaces: Set[(String, Boolean)],
      classNames: Set[String],
      hierarchy: EntityHierarchyManager): MatchTable = buildImpl(namespaces, classNames, hierarchy)

  private[temporalSurface] def buildImpl(
      namespaces: Set[(String, Boolean)],
      classNames: Set[String],
      hierarchy: EntityHierarchyManager = defaultEntityHierachyManager): MatchTable = {
    val classNamesMetaData = classNames.flatMap(c => hierarchy.metaData.get(c))
    val allClassesInPackage = hierarchy.metaData.values.filter(md => inOneNamespace(md, namespaces)).toSeq

    getMatchTableForClassMetas(namespaces, classNames, classNamesMetaData ++ allClassesInPackage, hierarchy = hierarchy)
  }

  private[optimus] def union(tables: Seq[MatchTable]): MatchTable = {
    val classes = tables.flatMap(_.classNames).toSet
    val namespaces = tables.flatMap(_.namespaces).toSet
    val hierarchies = tables.map(_.hierarchy).distinct
    assert(hierarchies.size <= 1, "cross-universe MatchTable union... this should never happen")
    val hierarchy = hierarchies.headOption.getOrElse(defaultEntityHierachyManager)
    build(namespaces, classes, hierarchy)
  }

  /**
   * Generate match table from a collection of EntityMetaData.
   *
   * * The classes represented by the initial collection are considered as AlwaysMatch. * All the descendent classes and
   * traits of the initial collection are considered as AlwaysMatch.
   *
   * We firstly expand the initial collection to contain all the descendents.
   *
   * We visit the entity ancestors (user defined) of the classes in the initial collection to build a parent-to-child
   * dependency graph. Note that not all the children of the ancestors will be present in the dependency graph. Consider
   * the following hierarchy:
   *
   * A / \ B C
   *
   * If we start with Seq(C), we will only have HashMap(A -> HashSet(C)) for the depenency graph.
   *
   * Now we visit the depenency graph in topological order, with the rule that a class has no dependency if its match
   * category (AlwaysMatch, CantTell) is determined.
   *
   * We process each class in the workQueue. If the class has a determined match category, we remove it from the
   * dependency of its parent. If after that the parent has no dependency, we add the parent to the workQueue.
   *
   * A class is in AlwaysMatch category if * it's in the initial collection * or all of its concrete storable
   * descendents (including itself) are in the AlwaysMatch category A class is in CantTell category if * it's not in the
   * initial collection * and not all of its concrete storable descendents (including itself) are in the AlwaysMatch
   * category
   */
  private def getMatchTableForClassMetas(
      namespaces: Set[(String, Boolean)],
      classNames: Set[String],
      classes: Set[ClassMetaData],
      includesSelf: Boolean = true,
      hierarchy: EntityHierarchyManager = defaultEntityHierachyManager): MatchTable = {
    val parentToChild = new mutable.HashMap[ClassMetaData, mutable.HashSet[ClassMetaData]]
    val alwaysMatch = new mutable.HashSet[ClassMetaData]
    val cantTell = new mutable.HashSet[ClassMetaData]

    val workQueue = new mutable.Queue[ClassMetaData]

    // Compute the initial always match group
    // This will be:
    //   1. The classes passed in
    //   2. and all the descendents of those classes
    // We may process these classes regardless of the dependency
    classes foreach { meta =>
      alwaysMatch += meta
      workQueue += meta
      meta.allChildren foreach { m =>
        alwaysMatch += m
        workQueue += m
      }
    }

    // Starting from the given class, we trace back to its parents and ancestors and
    // build a bi-directional graph for the child-parent dependency.
    // Note that we will not trace back to the children on a given class.
    // This means starting from A in the following hierarchy, we will build dependency for B.
    //         X
    //        / \
    //       B   A
    // Scala allows mixin multiple traits, which means there may be possible diamond inheritence of traits.
    val visited = mutable.HashSet.empty[ClassMetaData]
    def buildDependency(clz: ClassMetaData): Unit = {
      if (!visited(clz) && hierarchy.userDefined(clz)) {
        visited += clz
        clz.parents foreach { p =>
          if (hierarchy.userDefined(p)) {
            parentToChild.getOrElseUpdate(p, mutable.HashSet.empty) += clz
            buildDependency(p)
          }
        }
      }
    }

    // build depencency graph for all classes that is already determined on the match result.
    alwaysMatch foreach buildDependency

    // When a entity is known to be always match, we remove it from its parents' dependency lists.
    // If its parent's dependency list becomes empty, and the parent is still not determined, we move it to the workQueue.
    def markAlwaysMatch(e: ClassMetaData): Unit = {
      alwaysMatch += e
      e.parents.filter(hierarchy.userDefined) foreach { p =>
        parentToChild.get(p) foreach { pDeps =>
          pDeps -= e
          if (pDeps.isEmpty) {
            workQueue += p
          }
        }
      }
    }

    // When a entity is known to be can't tell, we remove it from its parents' dependency lists.
    // If its parent's dependency list becomes empty, and the parent is still not determined, we move it to the workQueue.
    // If any of the children is CantTell, the parent will be CantTell, so we mark it immediately.
    // The order here is important. We need to make sure the parent is first added into the workQueue and then marked as CantTell so that
    // we won't miss the check for the parent's parents.
    def markCantTell(e: ClassMetaData): Unit = {
      cantTell += e
      e.parents.filter(hierarchy.userDefined) foreach { p =>
        parentToChild.get(p) foreach { pDeps =>
          pDeps -= e
          if (pDeps.isEmpty) {
            workQueue += p
          }
        }
        cantTell += p
      }
    }

    while (workQueue.nonEmpty) {
      val e = workQueue.dequeue()

      if (alwaysMatch(e)) {
        // It's already in the AlwaysMatch category.
        // This happens for the classes in the initial collection.
        markAlwaysMatch(e)
      } else if (cantTell(e)) {
        // It's already in the CantTell category.
        // This happens because one of its children is marked as CantTell in some previous iteration.
        markCantTell(e)
      } else if (e.isStorableConcreteEntity) {
        // It's not yet determined, which means its the ancestor of one of the class in the initial collection.
        // It's itself a concrete class, query of the instance of this class falls out of the initial collection.
        // However, query of its descendent may fall in the initial collection.
        markCantTell(e)
      } else {
        val allStorableConcreteChildren = e.allChildren.filter(_.isStorableConcreteEntity)
        if (allStorableConcreteChildren.diff(alwaysMatch).isEmpty) {
          markAlwaysMatch(e)
        } else {
          markCantTell(e)
        }
      }
    }

    require(
      alwaysMatch.size + cantTell.size == visited.size,
      s"alwaysMatch.size ${alwaysMatch.size} + cantTell.size ${cantTell.size} != visited.size ${visited.size} - ${visited -- alwaysMatch -- cantTell} -- ${(cantTell ++ alwaysMatch) -- visited}"
    )

    cantTell += entityClassMetaData

    if (!includesSelf) {
      classes foreach { meta =>
        alwaysMatch -= meta
        cantTell -= meta
      }
    }

    createMatchTable(namespaces, classNames, hierarchy, alwaysMatch.toSet, cantTell.toSet)
  }
}
