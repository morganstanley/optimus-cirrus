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
package optimus.platform.runtime

import java.io.ByteArrayInputStream
import java.time.Instant

import com.ms.zookeeper.clientutils.ZkEnv
import msjava.hdom.Document
import msjava.hdom.Element
import msjava.hdom.input.SAXBuilder
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.OptimusConfigurationException
import org.apache.curator.framework.CuratorFramework

import scala.jdk.CollectionConverters._

trait XmlBasedConfiguration {
  val configs: Seq[Document]

  def getString(name: String): Option[String] = {
    getNodeData(name)
  }

  def getInt(name: String): Option[Int] = {
    getNodeData(name) map { _.toInt }
  }

  def getBoolean(name: String): Option[Boolean] = {
    getNodeData(name) map { _.toBoolean }
  }

  def getInstant(name: String): Option[Instant] = {
    getNodeData(name) map { s =>
      Instant.ofEpochMilli(s.toLong)
    }
  }

  def get(name: String): Option[Any] = {
    getString(name)
  }

  private[this] def tryAllConfigs[T](path: String, func: (Element, List[String]) => Option[T]): Option[T] = {
    var fieldValue: Option[T] = None
    val nodeList = path.split('.').toList

    configs find { config =>
      func(config.getRootElement, nodeList) match {
        case Some(x) =>
          fieldValue = Some(x)
          true
        case None => false
      }
    }
    fieldValue
  }

  private[this] def useAllConfigs[T](path: String, func: (Element, List[String]) => Option[T]): Seq[T] = {
    val nodeList = path.split('.').toList
    configs flatMap { config =>
      func(config.getRootElement, nodeList)
    }
  }

  private[this] def getNodeData(name: String): Option[String] = {
    tryAllConfigs(name, getNodeData)
  }

  def getStringList(name: String): Option[List[String]] = {
    tryAllConfigs(name, XmlBasedConfiguration.getStringList)
  }

  def getByteVector(name: String): Option[Vector[Byte]] = {
    getNodeData(name) map { _.getBytes.toVector }
  }

  def getProperties(baseName: String): Option[List[String]] = {
    tryAllConfigs(baseName, getProperties)
  }

  def getAllProperties(baseName: String): Option[Set[String]] = {
    val propertyNames = useAllConfigs(baseName, getProperties)
    val unqPropertyNames = propertyNames.flatten.toSet
    if (unqPropertyNames.isEmpty) None else Some(unqPropertyNames)
  }

  def getAttributes(name: String): Map[String, String] = tryAllConfigs(name, getNodeAttributes).getOrElse(Map.empty)

  def getStringListWithAttributes(name: String): List[(String, Map[String, String])] = {
    tryAllConfigs(name, getNodeDataWithAttributes).getOrElse(List.empty)
  }

  private[this] def getNodeData(node: Element, nodes: List[String]): Option[String] = {
    if (nodes.isEmpty) {
      node.getText match {
        case ""        => None
        case s: String => Some(s)
      }
    } else {
      val children = node.getChildren.asScala
      val child = children.find(_.getName == nodes(0))
      (child map { getNodeData(_, nodes.drop(1)) }).getOrElse(None)
    }
  }

  private[this] def getProperties(node: Element, nodes: List[String]): Option[List[String]] = {
    if (nodes.isEmpty) {
      val children = node.getChildren
      Some((children.asScala map { _.getName }).toList)
    } else {
      val children = node.getChildren.asScala
      val child = children.find(_.getName == nodes(0))
      child match {
        case Some(n) => getProperties(n, nodes.drop(1))
        case None    => None
      }
    }
  }

  private def getNodeAttributes(node: Element, nodes: List[String]): Option[Map[String, String]] = {
    if (nodes.isEmpty) {
      val attributes = node.getAttributes.asScala
      Some(attributes.map { attr =>
        attr.getName -> attr.getValue
      }.toMap)
    } else {
      val children = node.getChildren.asScala
      val child = children.find(_.getName == nodes.head)
      (child map { getNodeAttributes(_, nodes.drop(1)) }).getOrElse(None)
    }
  }

  private def getNodeDataWithAttributes(
      node: Element,
      nodes: List[String]): Option[List[(String, Map[String, String])]] = {
    if (nodes.size == 1) {
      val data = node.getChildren(nodes.head).asScala.toList map { node =>
        val attributes = node.getAttributes.asScala.map { attr =>
          attr.getName -> attr.getValue
        }
        node.getText -> attributes.toMap
      }
      if (data.isEmpty) None else Some(data)
    } else {
      val children = node.getChildren.asScala
      val child = children.find(_.getName == nodes.head)
      child match {
        case Some(n) => getNodeDataWithAttributes(n, nodes.drop(1))
        case None    => None
      }
    }
  }

  def getRootProperty: String = {
    require(configs.size == 1, s"One document is expected. Got configs = ${configs}")
    configs.head.getRootElement.getName
  }
}

object XmlBasedConfiguration {
  private[optimus] def getStringList(node: Element, nodes: List[String]): Option[List[String]] = {
    if (nodes.size == 1) {
      val children = (node.getChildren(nodes(0)).asScala map { _.getText }).toList
      // node.getChildren will return an empty list if there are no nested elements with the given name
      if (children.isEmpty) None else Some(children)
    } else {
      val children = node.getChildren.asScala
      val child = children.find(_.getName == nodes(0))
      child match {
        case Some(n) => getStringList(n, nodes.drop(1))
        case None    => None
      }
    }
  }
}

object ZkXmlConfigurationLoader {
  private[this] val log = getLogger[ZkXmlConfigurationLoader.type]

  def readConfig(path: String, zookeeperEnv: ZkEnv = ZkEnv.prod, timer: ZkOpsTimer): XmlBasedConfiguration = {
    new XmlBasedConfiguration {
      val configs = Seq(readConfigDoc(path, zookeeperEnv, timer))
    }
  }

  def readConfig(path: String, curator: CuratorFramework, timer: ZkOpsTimer): XmlBasedConfiguration = {
    new XmlBasedConfiguration {
      val configs = Seq(readConfigDoc(path, curator, timer))
    }
  }

  private[optimus] def pathExists(path: String, zookeeperEnv: ZkEnv = ZkEnv.prod): Boolean = {
    val curator = ZkUtils.getRootContext(zookeeperEnv).getCurator
    pathExists(path, curator)
  }

  private def pathExists(path: String, curator: CuratorFramework): Boolean = {
    curator.checkExists.forPath(path) != null
  }
  private[optimus] def pathExistsForTest(path: String, curator: CuratorFramework): Boolean =
    pathExists(path, curator)

  private[optimus] def readConfigDoc(path: String, zookeeperEnv: ZkEnv = ZkEnv.prod, timer: ZkOpsTimer): Document = {
    val curator = timer.timed("getRootContext") { ZkUtils.getRootContext(zookeeperEnv, false).getCurator }
    readConfigDoc(path, curator, timer)
  }

  private def readConfigDoc(path: String, curator: CuratorFramework, timer: ZkOpsTimer): Document = {
    try {
      log.info(s"Reading ${path}")
      val data = timer.timed(s"readConfigDoc($path)") { curator.getData.forPath(path) }
      new SAXBuilder().build(new ByteArrayInputStream(data))
    } catch {
      // The only possible zk related exception is KeeperException.ConnectionLossException when the connection to zk couldn't be really established
      // within the connection timeout (after all the retries as specified in RetryPolicy in ZkConfigClient
      case x: Exception =>
        throw new OptimusConfigurationException(
          Some(s"Could not read configuration from ZooKeeper (path: ${path})"),
          Some(x))
    }
  }
  private[optimus] def readConfigDocForTest(path: String, curator: CuratorFramework, timer: ZkOpsTimer): Document =
    readConfigDoc(path, curator, timer)

  def getStringListFromConfigPath(
      propertyName: String,
      configPath: String,
      zookeeperEnv: ZkEnv = ZkEnv.prod,
      timer: ZkOpsTimer): Option[List[String]] = {
    val nodeList = propertyName.split('.').toList
    XmlBasedConfiguration.getStringList(
      readConfig(configPath, zookeeperEnv, timer).configs.head.getRootElement,
      nodeList)
  }
}
