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
package optimus.dal.silverking.client

import optimus.dal.silverking.SilverKingPlantConfig
import optimus.dal.silverking.SysLoc
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalServicesSilverKingLookup
import optimus.platform.runtime.XmlBasedConfiguration
import optimus.platform.runtime.ZkOpsTimer
import optimus.platform.runtime.ZkUtils
import optimus.platform.runtime.ZkXmlConfigurationLoader

private[optimus] object PrcServiceConfig {
  private def rootProp = "silverking"
  private def plantsProp = "plants"
  private def dhtProp = "skDhtName"
  private def clusterNameProp = "skClusterName"
  private def versionProp = "version"
  private def namespaceProp = "namespace"
  private def fixedVersion = "0000000000"

  private def getProperty(config: XmlBasedConfiguration, path: String, default: => Option[String]) =
    config.getString(path).orElse(default).getOrElse(throw new IllegalArgumentException(s"Missing property $path"))

  private[optimus] def getAllPlants(config: XmlBasedConfiguration): List[String] =
    config.getProperties(s"$rootProp.$plantsProp").getOrElse(List.empty)

  private[optimus] def parsePlantConfig(
      config: XmlBasedConfiguration,
      plantName: String,
      env: DalEnv): SilverKingPlantConfig = {
    val plants = s"$rootProp.$plantsProp"
    val path = s"$plants.$plantName"

    val allPlants = getAllPlants(config)

    if (!allPlants.contains(plantName)) {
      throw new IllegalArgumentException(s"No PRC cluster with name $plantName configured in node")
    }

    val dht = getProperty(config, s"$path.$dhtProp", None)
    val clusterName = getProperty(config, s"$path.$clusterNameProp", None)
    val version = getProperty(config, s"$path.$versionProp", Some(fixedVersion))

    SilverKingPlantConfig(ZkUtils.getZkEnv(env), clusterName, dht, version, SysLoc.current)
  }

  private[optimus] def parseNamespace(
      config: XmlBasedConfiguration,
      plantName: String,
      env: DalEnv): PrcNamespaceTemplate = {
    val plants = s"$rootProp.$plantsProp"
    val path = s"$plants.$plantName"
    val nsTemplate = getProperty(config, s"$path.$namespaceProp", None)
    PrcNamespaceTemplate(nsTemplate)
  }

  private[optimus] def readZkConfig(
      skLookup: DalServicesSilverKingLookup): (SilverKingPlantConfig, PrcNamespaceTemplate) = {
    val nodePath = ZkUtils.getSilverKingServicesNode(skLookup.env.mode, skLookup.instance)
    val config = ZkXmlConfigurationLoader.readConfig(nodePath, ZkUtils.getZkEnv(skLookup.env), ZkOpsTimer.getDefault)
    val cfg = parsePlantConfig(config, skLookup.plantName, skLookup.env)
    val nsTemplate = parseNamespace(config, skLookup.plantName, skLookup.env)
    (cfg, nsTemplate)
  }

  private[optimus] def readPlantConfig(skLookup: DalServicesSilverKingLookup): SilverKingPlantConfig = {
    val config = readXMLConfig(skLookup.env.mode, skLookup.instance)
    parsePlantConfig(config, skLookup.plantName, skLookup.env)
  }

  private[optimus] def readXMLConfig(envMode: String, instance: String): XmlBasedConfiguration = {
    val nodePath = ZkUtils.getSilverKingServicesNode(envMode, instance)
    ZkXmlConfigurationLoader.readConfig(nodePath, ZkUtils.getZkEnv(DalEnv(envMode)), ZkOpsTimer.getDefault)
  }

  private[optimus] def readAllPlantConfig(
      envMode: String,
      instance: String): Seq[(SilverKingPlantConfig, PrcNamespaceTemplate)] = {
    val config = readXMLConfig(envMode, instance)
    readAllPlantConfig(config, envMode)
  }

  private[optimus] def readAllPlantConfig(
      config: XmlBasedConfiguration,
      envMode: String): Seq[(SilverKingPlantConfig, PrcNamespaceTemplate)] = {
    val allPlants = getAllPlants(config)
    allPlants map { plant =>
      val cfg = parsePlantConfig(config, plant, DalEnv(envMode))
      val nsTemplate = parseNamespace(config, plant, DalEnv(envMode))
      (cfg, nsTemplate)
    }
  }
}
