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

import com.ms.zookeeper.clientutils.ZkEnv
import msjava.hdom.Document
import optimus.breadcrumbs.ChainedID
import optimus.config.RuntimeConfiguration
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.ParCollectionConverters._

class ZkRuntimeConfiguration(
    override val mode: String,
    override val instance: Option[String],
    override protected val zookeeperEnv: ZkEnv,
    zkPathPrefix: Option[String] = None
) extends RuntimeConfiguration
    with XmlBasedConfiguration
    with HasZookeeperEnv
    with WithZkOpsMetrics {

  val DefaultNode = System.getProperty("optimus.dsi.ZKConfig.DefaultNode", "_")
  val RootElementName = System.getProperty("optimus.dsi.ZKConfig.RootElementName", "config")
  val RetryCount = Integer.getInteger("optimus.dsi.ZKConfig.RetryCount", 10)
  val RetryDelay = Integer.getInteger("optimus.dsi.ZKConfig.RetryDelay", 500)

  override def specificPropertyMap = Map(
    "zkEnv" -> zookeeperEnv.name,
    "zkMode" -> mode,
    "zkInstance" -> instance.getOrElse("none"),
    "zkDefaultNode" -> DefaultNode,
    "zkRootElementName" -> RootElementName
  )

  lazy val configs = withZkOpsMetrics(rootID) { timer =>
    loadAllConfigs(timer)
  }

  def loadAllConfigs(timer: ZkOpsTimer): Seq[Document] = {
    val globalDefault = readConfigDoc("/%s".format(DefaultNode), timer)
    val path = {
      val p = if (instance.isDefined) "%s/%s".format(mode, instance.get) else mode
      zkPathPrefix
        .map { prefix =>
          "%s/%s".format(prefix, p)
        }
        .getOrElse(p)
    }
    val partialPaths = {
      var partialPath = ""
      path.split("/").map { node =>
        partialPath = "%s/%s".format(partialPath, node)
        partialPath
      }
    }
    val configs = partialPaths.toIndexedSeq.par.map { partialPath =>
      readConfigDoc(partialPath, timer)
    }.seq
    // if the ZK path is /dev/zoneid_1/na/vi
    // configurations are stored in reverse order
    configs.reverse ++ Seq(globalDefault)
  }

  protected def readConfigDoc(path: String, timer: ZkOpsTimer): Document =
    ZkXmlConfigurationLoader.readConfigDoc(path, zookeeperEnv, timer)
  protected def pathExists(path: String): Boolean =
    ZkXmlConfigurationLoader.pathExists(path, zookeeperEnv)

  override def env =
    instance
      .map { i: String =>
        s"$mode:$i"
      }
      .getOrElse(mode)
  override val rootID = ChainedID.create()

  override def withOverride(name: String, value: Any) = new ConfigOverrider(this, Map(name -> value))
  override def withOverrides(m: Map[String, Any]) = new ConfigOverrider(this, m)
}
