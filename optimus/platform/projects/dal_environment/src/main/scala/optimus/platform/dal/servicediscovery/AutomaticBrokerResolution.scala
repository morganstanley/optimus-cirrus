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
package optimus.platform.dal.servicediscovery

import com.ms.zookeeper.clientutils.ZkEnv
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.OptimusConfigurationException
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalZoneId
import optimus.platform.runtime.SharedRuntimeProperties
import optimus.platform.runtime.ZkOpsTimer
import optimus.platform.runtime.ZkXmlConfigurationLoader
import org.apache.curator.framework.CuratorFramework

private[optimus] trait AbrZkOperations {
  def pathExists(path: String, timer: ZkOpsTimer): Boolean
  def getStringList(propertyName: String, configPath: String, timer: ZkOpsTimer): Option[List[String]]
}

private[optimus] final class CuratorAbrZkOperations(zkCurator: CuratorFramework, zkEnv: ZkEnv) extends AbrZkOperations {
  override def pathExists(path: String, timer: ZkOpsTimer): Boolean =
    timer.timed(s"pathExists($path)") {
      Option(zkCurator.checkExists.forPath(path)).isDefined
    }

  override def getStringList(propertyName: String, configPath: String, timer: ZkOpsTimer): Option[List[String]] =
    ZkXmlConfigurationLoader.getStringListFromConfigPath(propertyName, configPath, zkEnv, timer)
}

private[optimus] object AutomaticBrokerResolution {
  private val log = getLogger(AutomaticBrokerResolution)

  /*
   * Given a DalEnv and some information about a client, return the DalEnv to which the client should connect.
   *
   * The "mode" part of the input DalEnv is guaranteed to be preserved by automatic broker resolution. However, the
   * "instance" part is eligible to change. For example, an input of DalEnv("dev") might result in a return value of
   * DalEnv("dev") or DalEnv("dev:ln"), but could not result in a return value of DalEnv("qa") or DalEnv("qa:ln").
   *
   * The "instance" part of the input DalEnv is _not_ guaranteed to be preserved -- a caller could supply
   * DalEnv("dev:ln") and get back DalEnv("dev:ny").
   */
  def resolveSpecificDalInstance(
      inputEnv: DalEnv,
      inputZone: DalZoneId,
      sysLocOpt: Option[String],
      zkCurator: CuratorFramework,
      zkEnv: ZkEnv,
      timer: ZkOpsTimer): DalEnv = {
    resolveSpecificDalInstance(inputEnv, inputZone, sysLocOpt, new CuratorAbrZkOperations(zkCurator, zkEnv), timer)
  }

  def resolveSpecificDalInstance(
      inputEnv: DalEnv,
      inputZone: DalZoneId,
      sysLocOpt: Option[String],
      zkOps: AbrZkOperations,
      timer: ZkOpsTimer): DalEnv = {
    val (mode, region) = inputEnv.components

    val abrZone = getABRZoneForInputZone(inputZone, mode, zkOps, timer)

    region match {
      case Some(givenInstance) =>
        // no need to do any resolution if exact zookeeper path is given e.g. -e dev:<zoneId>/eu/ln
        if (zkOps.pathExists(s"/$mode/$givenInstance", timer)) {
          log.info(
            s"[ABR] no DAL resolution required as exact path is found in Zookeeper for mode: $mode instance: $region")
          inputEnv
        } else {
          val mappings = zkOps.getStringList(SharedRuntimeProperties.DsiCityContinentMappingProperty, s"/$mode", timer)
          mappings.getOrElse(Nil) match {
            case Nil => throw new OptimusConfigurationException(s"no mappings defined in path: $mode")
            case ms =>
              val pairs = ms.map { m =>
                val split = m.split(":")
                if (split.length != 2)
                  throw new OptimusConfigurationException(s"bad mapping in ZooKeeper (path: $mode mapping: $m)")
                (split(0), split(1))
              }
              val cityContinentMap = pairs.toMap
              if (cityContinentMap.get(givenInstance).isEmpty)
                throw new OptimusConfigurationException(s"invalid environment: $mode:$givenInstance")
              val mappedInstance =
                s"${abrZone.underlying}/${cityContinentMap(givenInstance)}/$givenInstance"
              log.info(
                s"[ABR] resolved to a DAL instance in Zookeeper from: ${inputEnv.underlying}, to: /$mode/$mappedInstance")
              inputEnv.atInstance(mappedInstance)
          }
        }
      case None =>
        val resolvedInstance = doAutomaticDALResolution(mode, abrZone, sysLocOpt, zkOps, timer)
        resolvedInstance
          .map { instance =>
            log.info(
              s"[ABR] resolved automatically to a DAL instance in Zookeeper from: ${inputEnv.underlying}, to: /$mode/$instance")
            inputEnv.atInstance(instance)
          }
          .getOrElse {
            log.warn(
              s"[ABR] couldn't resolve automatically to a DAL instance in Zookeeper, reading only ${inputEnv.underlying} node from zk")
            inputEnv
          }
    }
  }

  /*
   * Given some input zone ID, look up the ABR zone ID in Zoo Keeper.
   */
  private def getABRZoneForInputZone(
      inputZone: DalZoneId,
      mode: String,
      zkOps: AbrZkOperations,
      timer: ZkOpsTimer): DalZoneId = {
    if (zkOps.pathExists(s"/$mode/${inputZone.underlying}", timer)) inputZone
    else {
      log.info(s"[ABR] no client automatic broker resolution override for zone $inputZone.")
      SharedRuntimeProperties.DsiZonePropertyDefaultValue
    }
  }

  /**
   * this will do the automatic DAL resolution based on the city, continent, zoneId and mode of the client sysLocOpt is
   * expected to be a dot-delimited string of 0 or more arbitrary values ending with building.city.continent (e.g. the
   * SYS_LOC environment variable) zone and mode will be specified by client at startup
   */
  private def doAutomaticDALResolution(
      mode: String,
      zone: DalZoneId,
      sysLocOpt: Option[String],
      zkOps: AbrZkOperations,
      timer: ZkOpsTimer): Option[String] = {
    sysLocOpt match {
      case None =>
        log.warn("[ABR] no sysloc provided for localized broker resolution")
        None
      case Some(sysloc) =>
        val split = sysloc.split("\\.")

        /**
         * SYS_LOC - dot separated list of 0 or more arbitrary values followed by building, city, continent
         */
        if (split.length < 3) {
          log.warn(s"[ABR] not able to parse sysloc: $sysloc")
          None
        }
        val continent = split(split.length - 1)
        val city = split(split.length - 2)
        log.info(
          s"[ABR] doing automatic DAL resolution for mode: $mode zoneId: ${zone.underlying} continent: $continent city: $city")
        if (zkOps.pathExists(s"/$mode/${zone.underlying}/$continent/$city", timer))
          Some(s"${zone.underlying}/$continent/$city")
        else if (zkOps.pathExists(s"/$mode/${zone.underlying}/$continent", timer))
          Some(s"${zone.underlying}/$continent")
        else if (zkOps.pathExists(s"/$mode/${zone.underlying}", timer))
          Some(s"${zone.underlying}")
        else None
    }
  }
}
