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
package optimus.dsi.partitioning

import java.util.concurrent.ConcurrentHashMap

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.ms.zookeeper.clientutils.ZkEnv
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.config.OptimusConfigurationException
import optimus.platform.dal.config.DalEnv
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.runtime.WithZkOpsMetrics
import optimus.platform.runtime.XmlBasedConfiguration
import optimus.platform.runtime.ZkOpsTimer
import optimus.platform.runtime.ZkUtils
import optimus.platform.runtime.ZkXmlConfigurationLoader
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import PartitionMap.TypeRef

trait PartitionMap {
  def partitions: Set[NamedPartition]
  def allPartitions: Set[Partition]
  def partitionForType(typ: TypeRef): Partition
  def partitionForType(clz: Class[_]): Partition
  def partitionTypeRefMap: Map[Partition, Set[TypeRef]]
  def isEmpty: Boolean
  def validate(typ: TypeRef): Unit
}

object PartitionMap {
  private val PartitionRootProp = "partitions"
  private val wordRegex = "\\w".r
  private val wildcardChar = "*"
  private[this] val log = getLogger[PartitionMap.type]
  val PartitionMapProperty = "optimus.dsi.partitioning.PartitionMap"
  private[optimus] val empty: PartitionMap = new PartitionMapImpl(Set.empty, Map.empty, Set.empty)
  private[partitioning] type TypeRef = String

  private lazy val cache = {
    CacheBuilder
      .newBuilder()
      .build(new CacheLoader[(String, ZkEnv), PartitionMap] with WithZkOpsMetrics {
        override def load(dalAndZkEnv: (String, ZkEnv)): PartitionMap = {
          val env = dalAndZkEnv._1
          if (env == "mock") PartitionMap.empty
          else {
            val zkEnv = dalAndZkEnv._2
            val path = s"/partitions/$env"

            Try(withZkOpsMetrics(ChainedID.root) { timer =>
              ZkXmlConfigurationLoader.readConfig(path, zkEnv, timer)
            }) match {
              case Success(config) => createPartitionMap(config, env)
              case Failure(ex) =>
                log.info(
                  s"Could not read partition map config at ZK path: $path, setting it as empty. " +
                    s"(error message: ${Option(ex.getCause).map(_.getMessage).getOrElse("<no cause could be found>")})"
                )
                PartitionMap.empty
            }
          }
        }
      })
  }

  private def getOverlappingRules(wildcards: Seq[String], classNames: Seq[String]): Seq[Set[String]] = {
    val map = mutable.Map.empty[String, Set[String]]
    wildcards.combinations(2).foreach { case Seq(w1, w2) =>
      val ns1 = w1.substring(0, w1.length - 1)
      val ns2 = w2.substring(0, w2.length - 1)
      if (ns1.startsWith(ns2) || ns2.startsWith(ns1)) {
        val existingSet = map.getOrElse(s"$ns1$wildcardChar", Set.empty)
        map.update(s"$ns1$wildcardChar", existingSet + s"$ns2$wildcardChar")
      }
    }

    wildcards.foreach { w =>
      val ns = w.substring(0, w.length - 1)
      classNames.foreach { className =>
        if (className.startsWith(ns)) {
          val existingSet = map.getOrElse(s"$ns$wildcardChar", Set.empty)
          map.update(s"$ns$wildcardChar", existingSet + className)
        }
      }
    }
    map.map { case (k, v) => v + k }.toSeq
  }

  private def createPartitionMap(config: XmlBasedConfiguration, env: String = "test"): PartitionMap = {
    config.getProperties(PartitionRootProp) match {
      case None =>
        log.info(s"No type to partition mapping config found for env $env.")
        new PartitionMapImpl(Set.empty, Map.empty, Set.empty)
      case Some(partitionStrings) =>
        if (partitionStrings.size != partitionStrings.toSet.size)
          throw new OptimusConfigurationException("More than one tag for a partition found, expected only one.")
        val partitions = partitionStrings.map(NamedPartition.apply).toSet
        val (typePartitionPairs, ignoredTypeRefSet) =
          partitions.foldLeft((Map.empty[TypeRef, NamedPartition], Set.empty[TypeRef])) {
            case ((typePairs, ignoredSet), p) =>
              val typeProp = s"$PartitionRootProp.${p.name}.type"
              config.getStringList(typeProp) match {
                case None =>
                  throw new OptimusConfigurationException(
                    s"No types mapped to partition '${p.name}'. Is this expected?")
                case Some(types) =>
                  val attributes = config.getStringListWithAttributes(typeProp)
                  val typesWithIgnoreCheck = attributes.foldLeft(Set.empty[TypeRef]) {
                    case (acc, (t, attribute)) if attribute.getOrElse("ignoreOverlapCheck", "false") == "true" =>
                      acc + t
                    case (acc, _) => acc

                  }
                  (types.map(_ -> p).toMap ++ typePairs, ignoredSet ++ typesWithIgnoreCheck)
              }

          }

        log.info(s"Type to partition mapping config is present for env $env. Partitions: $partitions")
        log.info(s"Ignored TypeRefs for overlapping are $ignoredTypeRefSet")

        new PartitionMapImpl(partitions, typePartitionPairs, ignoredTypeRefSet)
    }
  }

  // We *cannot* support different partition config at regional level.
  // That's why we just care for the "env" part of DALEnv (i.e., mode) and not region part (i.e, instance).
  // ***** Don't use this method to resolve PartitionMap on the client side. *****
  // The RuntimeConfiguration already contains PartitionMap under the property PartitionMap.PartitionMapProperty
  def apply(env: DalEnv): PartitionMap = cache.get(env.mode -> ZkUtils.getZkEnv(env))

  def apply(map: Map[TypeRef, NamedPartition] = Map.empty): PartitionMap =
    if (map.isEmpty) empty
    else new PartitionMapImpl(map.values.toSet, map, Set.empty)

  // Specifically for tests..
  def apply(path: String, curator: CuratorFramework): PartitionMap = {
    val config = ZkXmlConfigurationLoader.readConfig(path, curator, ZkOpsTimer.noop)
    createPartitionMap(config)
  }
  def apply(config: XmlBasedConfiguration): PartitionMap = {
    createPartitionMap(config)
  }

  def apply(partitionMap: PartitionMap, unavailablePartitions: Set[String]): PartitionMap = {
    if (unavailablePartitions.isEmpty) partitionMap
    else
      partitionMap match {
        case r: RestrictedPartitionMap =>
          new RestrictedPartitionMap(r.partitionMap, r.unavailablePartitions ++ unavailablePartitions)
        case pm => new RestrictedPartitionMap(pm, unavailablePartitions)
      }
  }

  private class RestrictedPartitionMap(val partitionMap: PartitionMap, val unavailablePartitions: Set[String])
      extends PartitionMap {
    val partitions: Set[NamedPartition] =
      partitionMap.partitions.filterNot(p => unavailablePartitions.contains(p.name))
    val allPartitions: Set[Partition] =
      partitionMap.allPartitions.filterNot(p => unavailablePartitions.contains(p.name))
    def partitionForType(typ: TypeRef): Partition = {
      val p = partitionMap.partitionForType(typ)
      if (!unavailablePartitions.contains(p.name)) p
      else throw new IllegalArgumentException(s"attempt to access unavailable partition $p")
    }
    def partitionForType(clz: Class[_]): Partition = {
      val p = partitionMap.partitionForType(clz)
      if (!unavailablePartitions.contains(p.name)) p
      else throw new IllegalArgumentException(s"attempt to access unavailable partition $p")
    }
    val partitionTypeRefMap: Map[Partition, Set[TypeRef]] = {
      partitionMap.partitionTypeRefMap.filterNot { case (p, _) => unavailablePartitions.contains(p.name) }
    }
    def isEmpty: Boolean = partitions.isEmpty
    def validate(typ: TypeRef): Unit = {
      partitionForType(typ)
    }
  }

  /**
   * This class contains mapping between type-name and partitions. This interface is supposed to be shared by both dal
   * client and server counterparts.
   */
  private class PartitionMapImpl(
      val partitions: Set[NamedPartition],
      typeMap: Map[TypeRef, Partition],
      ignoreOverlapCheckForTypes: Set[TypeRef])
      extends PartitionMap {
    // checks we need to when we create the PartitionMap
    if (typeMap.nonEmpty) {
      typeMap.keys.foreach { i =>
        val lastWord = i.split("\\.").last
        if (!(lastWord == PartitionMap.wildcardChar || PartitionMap.wordRegex.findFirstIn(lastWord).nonEmpty)) {
          throw new OptimusConfigurationException(
            s"The last word of type can either be $wildcardChar or a classname. Found type names = ${typeMap.keys}")
        }
      }

      val typesToCheckForOverlap = typeMap.keySet.diff(ignoreOverlapCheckForTypes)

      // We send only those type-refs for overlapping check which are not ignored
      val (wildcards, classNames) = typesToCheckForOverlap.toSeq.partition(_.endsWith(wildcardChar))
      val overlappingRules = getOverlappingRules(wildcards, classNames)
      if (overlappingRules.nonEmpty) {
        throw new OptimusConfigurationException(
          "Found overlapping Partition Rules. Partition Rules = " +
            s"${typeMap.keys.toSeq.mkString(", ")}. " +
            "OverlappingRules = " +
            s"${overlappingRules.mkString(", ")}")
      }
    }

    private val classNames = typeMap.keySet.filterNot(_.endsWith(wildcardChar))
    // We need to reverse sort the regex list as we need the most definate first; finding the match with highest specificity
    private val sortedTypeRegex =
      typeMap.keySet.diff(classNames).map(k => s"$k".r).toList.sortBy(_.toString.length).reverse

    val allPartitions: Set[Partition] = partitions.asInstanceOf[Set[Partition]] + DefaultPartition

    private[this] val partitionForTypeCache = new ConcurrentHashMap[TypeRef, Partition]()
    def partitionForType(typ: TypeRef): Partition =
      partitionForTypeCache.computeIfAbsent(
        typ,
        t =>
          classNames
            .find(_ == t)
            .map(r => typeMap(r))
            .orElse(
              sortedTypeRegex.find(r => r.findPrefixMatchOf(t).nonEmpty).map(r => typeMap(r.regex))
            )
            .getOrElse(DefaultPartition)
      )

    def partitionForType(clz: Class[_]): Partition = partitionForType(clz.getName)

    val partitionTypeRefMap: Map[Partition, Set[TypeRef]] =
      typeMap.groupBy(_._2).map { case (p, nss) =>
        p -> (nss.map { case (ns, _) => if (ns.endsWith(wildcardChar)) ns.dropRight(2) else ns } toSet)
      }

    def isEmpty: Boolean = partitions.isEmpty

    def validate(typ: TypeRef): Unit = {}
  }
}

class PartitionMapState {
  private var partitionMap: Option[Map[TypeRef, NamedPartition]] = None
}

object PartitionMapState extends SimpleStateHolder(() => new PartitionMapState) {
  def setPartitionMap(mapping: Map[TypeRef, NamedPartition]): Unit = getState.synchronized {
    getState.partitionMap = Some(mapping)
  }

  def getPartitionMapOpt = getState.partitionMap

  def clearPartitionMap: Unit = getState.synchronized { getState.partitionMap = None }
}
