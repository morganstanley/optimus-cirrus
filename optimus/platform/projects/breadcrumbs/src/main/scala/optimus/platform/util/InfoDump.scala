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

package optimus.platform.util
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.utils.MiscUtils.Endoish._

trait InfoDump {
  def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit

  def dump(
      prefix: String,
      msgs: List[String] = Nil,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      id: ChainedID = ChainedID.root.child,
      description: String = "Information",
      heapDump: Boolean = false,
      taskDump: Boolean = false,
      noMore: Boolean = false,
      exceptions: Array[Throwable] = Array.empty,
      emergency: Boolean = false
  ): String

}

object InfoDump extends Log {

  def ensureInitialized(): Unit = {}

  lazy private val dumpers = {
    val ret = ServiceLoaderUtils.all[InfoDump].applyIf(_.isEmpty)(_ :+ Default)
    log.info(s"Loaded dumpers $ret")
    ret
  }

  private object Default extends InfoDump with Log {
    def kill(
        prefix: String,
        msgs: List[String] = Nil,
        code: Int = -1,
        crumbSource: Crumb.Source = Crumb.RuntimeSource,
        exceptions: Array[Throwable] = Array.empty[Throwable]): Unit = {
      val e = new RuntimeException("Fatal error")
      val msg =
        s"Fatal error prefix=$prefix, msgs=$msgs, code=$code, source=$crumbSource, exception=${exceptions.mkString(" ,")}"
      exceptions.foreach(e.addSuppressed)
      System.err.println(msg)
      e.printStackTrace()
      Breadcrumbs.error(ChainedID.root, PropertiesCrumb(_, crumbSource, Properties.logMsg -> msg))
      Runtime.getRuntime.exit(code)
      throw e
    }

    def dump(
        prefix: String,
        msgs: List[String] = Nil,
        crumbSource: Crumb.Source = Crumb.RuntimeSource,
        id: ChainedID = ChainedID.root.child,
        description: String = "Information",
        heapDump: Boolean = false,
        taskDump: Boolean = false,
        noMore: Boolean = false,
        exceptions: Array[Throwable] = Array.empty,
        emergency: Boolean = false): String = {
      val msg = s"prefix=$prefix, msgs=$msgs, source=$crumbSource,  exception=${exceptions.mkString(" ,")}"
      log.info(msg)
      Breadcrumbs.info(ChainedID.root, PropertiesCrumb(_, crumbSource, Properties.logMsg -> msg))
      msg
    }

  }

  def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit =
    dumpers.foreach(_.kill(prefix, msgs, code, crumbSource, exceptions))

  def dump(
      prefix: String,
      msgs: List[String] = Nil,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      id: ChainedID = ChainedID.root.child,
      description: String = "Information",
      heapDump: Boolean = false,
      taskDump: Boolean = false,
      noMore: Boolean = false,
      exceptions: Array[Throwable] = Array.empty,
      emergency: Boolean = false
  ): String = dumpers
    .map(_.dump(prefix, msgs, crumbSource, id, description, heapDump, taskDump, noMore, exceptions, emergency))
    .mkString(";")

}
