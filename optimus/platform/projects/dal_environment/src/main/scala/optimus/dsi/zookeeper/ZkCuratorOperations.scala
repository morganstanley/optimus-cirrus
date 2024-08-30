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
package optimus.dsi.zookeeper

import msjava.slf4jutils.scalalog.getLogger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat

import scala.jdk.CollectionConverters._

final case class ZNode(path: String) {
  require(path != null, "path cannot be null")
  require(!path.isEmpty, "path cannot be empty")
  require(!path.endsWith("/") || path == "/", "path should not end with '/' unless it's exactly '/'")
  override def toString = path
}

trait WithMutex {
  def withMutex[A](f: => A): A
  protected def zkProcessLock: InterProcessMutex
  protected def nonProdLockPath: String
}

trait ReadZkOperations {
  def exists(node: ZNode): Boolean
  def getChildren(node: ZNode): Seq[String]
  def getChildrenData(node: ZNode, children: List[String]): List[(String, Array[Byte])]
  def getData(node: ZNode): Array[Byte]
  def getStatNode(node: ZNode): Stat
}

trait ZkOperations extends ReadZkOperations {
  def createNode(node: ZNode, createParentsIfNeeded: Boolean = false, mode: CreateMode = CreateMode.PERSISTENT): String
  def createNodeWithData(
      node: ZNode,
      data: Array[Byte],
      createParentsIfNeeded: Boolean = false,
      mode: CreateMode = CreateMode.PERSISTENT): String

  def setData(node: ZNode, data: Array[Byte]): Unit
  def addChildren(node: ZNode, childrenWithData: List[(String, Array[Byte])], logEachAddition: Boolean = false): Unit

  def delete(node: ZNode): Unit
  def deleteNodeAlongWithChildren(node: ZNode): Unit
}

class CuratorZkOperations(val zk: CuratorFramework, val batch: Int = 500, protected val nonProdLockPath: String = "")
    extends ZkOperations
    with WithMutex {

  private val log = getLogger(this)
  override protected lazy val zkProcessLock = new InterProcessMutex(zk, nonProdLockPath)

  override def exists(node: ZNode): Boolean = zk.checkExists().forPath(node.path) != null
  override def createNode(
      node: ZNode,
      createParentsIfNeeded: Boolean = false,
      mode: CreateMode = CreateMode.PERSISTENT): String = {
    val create = zk.create()
    if (createParentsIfNeeded)
      create.creatingParentsIfNeeded()
    create.withMode(mode).forPath(node.path)
  }
  override def createNodeWithData(
      node: ZNode,
      data: Array[Byte],
      createParentsIfNeeded: Boolean = false,
      mode: CreateMode = CreateMode.PERSISTENT): String = {
    val create = zk.create()
    if (createParentsIfNeeded)
      create.creatingParentsIfNeeded()
    create.withMode(mode).forPath(node.path, data)
  }

  override def setData(node: ZNode, data: Array[Byte]): Unit = zk.setData().forPath(node.path, data)

  override def delete(node: ZNode): Unit = zk.delete().forPath(node.path)

  override def getChildren(node: ZNode): Seq[String] = zk.getChildren.forPath(node.path).asScala
  override def getChildrenData(node: ZNode, children: List[String]): List[(String, Array[Byte])] = {
    log.info(
      "Getting children data for node: {} using Zk: {}",
      node.path,
      zk.getZookeeperClient.getCurrentConnectionString)
    children map { c =>
      val fp = node.path + "/" + c
      c -> zk.getData.forPath(fp)
    }
  }
  override def getData(node: ZNode): Array[Byte] = zk.getData.forPath(node.path)
  override def getStatNode(node: ZNode): Stat = zk.checkExists().forPath(node.path)
  override def deleteNodeAlongWithChildren(node: ZNode): Unit = {
    if (exists(node))
      zk.delete().deletingChildrenIfNeeded().forPath(node.path)
  }

  override def addChildren(
      node: ZNode,
      childrenWithData: List[(String, Array[Byte])],
      logEachAddition: Boolean = false): Unit = {
    log.info("Adding children under node: {} using Zk: {}", node.path, zk.getZookeeperClient.getCurrentConnectionString)
    childrenWithData.grouped(batch) foreach { grp =>
      val ops = grp.map {
        case (childName, data) => {
          val path = s"${node.path}/$childName"
          if (logEachAddition) log.info(s"Creating node $path with data ${new String(data)}")
          zk.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(path, data)
        }
      }
      zk.transaction().forOperations(ops.asJava)
    }
  }

  override def withMutex[A](f: => A): A = {
    try {
      zkProcessLock.acquire()
      f
    } finally {
      zkProcessLock.release()
    }
  }
}
