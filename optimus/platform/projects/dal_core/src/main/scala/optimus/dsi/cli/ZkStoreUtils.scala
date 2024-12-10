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
package optimus.dsi.cli

import java.io.ByteArrayInputStream

import msjava.hdom.Document
import msjava.hdom.Element
// import msjava.hdom.Node
import msjava.hdom.input.SAXBuilder
/* import msjava.hdom.xpath.HDOMXPathUtils
import msjava.msxml.xpath.MSXPathUtils */
import optimus.platform.dsi.bitemporal.DSISpecificError
import optimus.platform.runtime.ZkUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.jdk.CollectionConverters._
import scala.util.Try

class ZkStoreUtils(private val curator: CuratorFramework, printFunc: String => Unit = println) extends HdomHelper {

  //
  // CHECK ZK XML
  //
  def checkXmlIsValid(zkPath: String): Boolean = {
    readFromZooKeeper(zkPath).isSuccess
  }

  //
  // PRINT ZOOKEEPER CONFIG
  //
  def printZkConfig(zkPath: String): Unit = {
    val hdom = readFromZooKeeper(zkPath)
    if (hdom.isSuccess)
      printFunc(documentToString(hdom.get))
  }

  def printZkConfig(zkPath: String, xPath: String): Unit = ??? /* {
    val hdom = readFromZooKeeper(zkPath)
    val compiledxPath = MSXPathUtils.compile(xPath)
    if (hdom.isSuccess) {
      val elements = findElements(hdom.get, compiledxPath)
      elements.foreach(e => printFunc(elementToString(e)))
    }
  } */

  //
  // FIND NODE(s)
  //
  def findNode(zkPath: String, xPath: String, nodeName: String): Try[List[String]] = ??? /* {
    readFromZooKeeper(zkPath) map { hdom =>
      (findElements(hdom, MSXPathUtils.compile(s"$xPath/$nodeName"), None), hdom)
    } map { case (s, _) =>
      s.map(_.getNodeValue)
    }
  } */

  //
  // ADD NEW NODE
  //
  def addNode(
      zkPath: String,
      xPath: String,
      nodeName: String,
      nodeData: String,
      skipWriteIfPresent: Boolean = false): Try[Unit] = ??? /* {
    readFromZooKeeper(zkPath) map { hdom =>
      (findElements(hdom, MSXPathUtils.compile(xPath + "/" + nodeName), Some(nodeData)), hdom)
    } map {
      case (s, _) if s.nonEmpty && skipWriteIfPresent =>
        throw new DSISpecificError(
          s"The node <${nodeName}>${nodeData}</${nodeName}> already exists in the configuration.")
      case (s, hdom) =>
        val newElement = new Element(nodeName)
        newElement.setValue(nodeData)
        val parentNode = HDOMXPathUtils.getFirstElement(MSXPathUtils.compile(xPath), hdom)
        parentNode.addContent(newElement)

        writeToZooKeeper(zkPath, hdom).get
        printFunc(s"Node <${nodeName}>${nodeData}</${nodeName}> successfully added.")
    }
  } */

  def addNodes(zkPath: String, xPath: String, nodeData: List[Element]): Try[Unit] = ??? /* {
    val nodeName = xPath.split("/").last
    readFromZooKeeper(zkPath) map { hdom =>
      (findElements(hdom, MSXPathUtils.compile(xPath), None), hdom)
    } map {
      case (s, _) if s.size > 1 =>
        throw new DSISpecificError(s"Multiple nodes <${nodeName}> exists in the configuration.")
      case (s, hdom) if s.size == 1 =>
        s.head.setContent((nodeData: Seq[Node]).asJava) // Element implements Node but Java List is invariant
        writeToZooKeeper(zkPath, hdom).get
        printFunc(s"Node <${nodeName}>${nodeData}</${nodeName}> successfully updated.")
      case (s, hdom) =>
        val newElement = new Element(nodeName)
        newElement.setContent((nodeData: Seq[Node]).asJava)

        val parentXPath = xPath.split("/").tail.dropRight(1).fold("")((res, el) => res + "/" + el)
        val parentNode = HDOMXPathUtils.getFirstElement(MSXPathUtils.compile(parentXPath), hdom)
        if (parentNode == null)
          throw new DSISpecificError(s"The xpath <${parentXPath}> does not exist.")
        else {
          parentNode.addContent(newElement)
          writeToZooKeeper(zkPath, hdom).get
          printFunc(s"Node <${nodeName}>${nodeData}</${nodeName}> successfully added.")
        }
    }
  } */

  //
  // DELETE NODE
  //
  def deleteNode(zkPath: String, xPath: String, targetValue: Option[String] = None): Try[Unit] = ??? /* {
    readFromZooKeeper(zkPath) map { hdom =>
      (findElements(hdom, MSXPathUtils.compile(xPath), targetValue), hdom)
    } map {
      case (s, _) if s.isEmpty =>
        throw new DSISpecificError(
          s"The xpath ${xPath} refers to a nonexistent element in the xml configuration. Please check xpath and try again.")
      case (s, hdom) =>
        s.foreach(t => t.detach())
        writeToZooKeeper(zkPath, hdom).get
        printFunc(s"${s.size} node(s) successfully deleted.")
    }
  } */

  //
  // CHANGE NODE VALUE
  //
  def changeNodeValue(
      zkPath: String,
      xPath: String,
      newValue: String,
      targetValue: Option[String] = None,
      createNew: Boolean = false): Try[Unit] = ??? /* {
    readFromZooKeeper(zkPath) map { hdom =>
      (findElements(hdom, MSXPathUtils.compile(xPath), targetValue), hdom)
    } map {
      case (s, _) if s.isEmpty && !createNew =>
        throw new DSISpecificError(
          s"The xpath ${xPath} refers to a nonexistent element in the xml configuration. Please check xpath and try again.")
      case (s, hdom) if s.isEmpty && createNew =>
        // Create new node..
        val nodeName = xPath.split("/").last
        val newElement = new Element(nodeName)
        newElement.setText(newValue)
        // Get parent node..
        val parentXPath = xPath.split("/").tail.dropRight(1).fold("")((res, el) => res + "/" + el)
        val parentNode = HDOMXPathUtils.getFirstElement(MSXPathUtils.compile(parentXPath), hdom)
        if (parentNode == null)
          throw new DSISpecificError(s"The xpath <${parentXPath}> does not exist.")
        else {
          parentNode.addContent(newElement)
          // Save it..
          writeToZooKeeper(zkPath, hdom).get
          printFunc(s"Node ${xPath} successfully added with a value ${newValue}.")
        }
      case (s, hdom) if s.size == 1 =>
        s.head.setNodeValue(newValue)
        writeToZooKeeper(zkPath, hdom).get
        printFunc(s"Value of node ${xPath} successfully changed to ${newValue}.")
      case (s, _) =>
        throw new DSISpecificError(
          s"The xpath ${xPath} with value ${targetValue} refers to more than one element. Please check xpath and try again.")
    }
  } */

  //
  // PRIVATE HELPERS
  //
  private[optimus] def readFromZooKeeper(zkPath: String): Try[Document] = Try {
    val data = curator.getData.forPath(zkPath)
    new SAXBuilder().build(new ByteArrayInputStream(data))
  }

  def writeToZooKeeper(zkPath: String, hdom: Document): Try[Unit] = Try {
    val data = documentToString(hdom).getBytes
    val stat = curator.checkExists.forPath(zkPath)
    if (stat == null) {
      // path doesn't exist, create it
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath, data)
    } else {
      // path exists, so just update the config
      curator.setData().forPath(zkPath, data)
    }
  }

  def cleanUpZooKeeperNodeConfig(zkPath: String): Try[Unit] = Try {
    val data: Array[Byte] = Array()
    val stat = curator.checkExists.forPath(zkPath)
    if (stat != null) {
      // path exists, clean up its data
      curator.setData().forPath(zkPath, data)
    }
  }

  def copyXPath(srcEnv: String, dstEnv: String, zkRootPath: String, xPath: String) = ??? /* {
    val zkPath = zkRootPath + "/" + srcEnv
    val doc = readFromZooKeeper(zkPath)
    if (doc.isSuccess) {
      val elem = findElements(doc.get, MSXPathUtils.compile(xPath), None) match {
        case elems if elems.isEmpty =>
          throw new DSISpecificError(
            s"The xpath ${xPath} refers to a nonexistent element in the xml configuration at source env [${srcEnv}].")
        case elems if elems.size == 1 =>
          elems.head
        case _ =>
          throw new DSISpecificError(
            s"The xpath ${xPath} refers to a more than one element in the xml configuration at source env [${srcEnv}].")
      }

      val zkDstPath = zkRootPath + "/" + dstEnv

      // If element has children nodes, then copy them accordingly (up to one level down). Otherwise
      // just copy the value..
      if (elem.getChildCount > 0) {
        val children = elem.getChildren.asScala
        val newChildNodes = children map { node =>
          val newElement = new Element(node.getName)
          newElement.setText(node.getText)
          newElement
        }
        addNodes(zkDstPath, xPath, newChildNodes.toList)
      } else
        changeNodeValue(zkDstPath, xPath, elem.getText, None, true)
    } else
      printFunc(s"The source environment [${srcEnv}] is not valid. Please try again.")
  } */
}
