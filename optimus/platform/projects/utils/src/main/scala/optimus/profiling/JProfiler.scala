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
package optimus.profiling

import java.lang.invoke.{MethodType => MT, MethodHandles}
import java.util.concurrent.atomic.AtomicInteger

import optimus.utils.PropertyUtils

import scala.util.control.NonFatal
import msjava.slf4jutils.scalalog.getLogger

/** Facade over the JProfiler API, which is non-free. */
object JProfiler {
  private[this] val logger = getLogger(this)

  val active = PropertyUtils.get("optimus.profiling.jprofiler", false)

  lazy val controller: Option[Controller] = if (active) {
    try
      Some {
        val lookup = MethodHandles.lookup
        val c_Controller = Class.forName("com.jprofiler.api.agent.Controller")
        val c_BookmarkColor = Class.forName("com.jprofiler.api.agent.Controller$BookmarkColor")
        val m_BookmarkColor_init =
          lookup.findConstructor(c_BookmarkColor, MT.fromMethodDescriptorString("(III)V", null))
        val m_Controller_addBookmark =
          lookup.findStatic(
            c_Controller,
            "addBookmark",
            MT.methodType(classOf[Unit], classOf[String], c_BookmarkColor, classOf[Boolean]))
        (description: String, color: BookmarkColor, dashed: Boolean) => {
          val jpColor = m_BookmarkColor_init.invoke(color.red, color.green, color.blue)
          m_Controller_addBookmark.invoke(description, jpColor, dashed)
        }
      }
    catch {
      case NonFatal(ex) =>
        logger.error(s"Can't initialize JProfiler", ex)
        None
    }
  } else None

  // wrapper so projects can avoid explicit dependency on Controller
  trait Controller {
    def addBookmark(description: String, color: BookmarkColor, dashed: Boolean): Unit
  }
  final case class BookmarkColor(red: Int, green: Int, blue: Int)

  private val bookmarkCount = new AtomicInteger(0)

  val red = if (active) BookmarkColor(255, 0, 0) else null
  val green = if (active) BookmarkColor(0, 255, 0) else null
  val yellow = if (active) BookmarkColor(255, 255, 0) else null
  val purple = if (active) BookmarkColor(153, 0, 153) else null
  val blue = if (active) BookmarkColor(0, 0, 255) else null
  val grey = if (active) BookmarkColor(128, 128, 128) else null

  def addBookmark(s: => String, color: BookmarkColor, dashed: Boolean): Int = {
    controller.fold(-1) { controller =>
      val n = bookmarkCount.incrementAndGet()
      controller.addBookmark(s"$n: $s", color, dashed)
      n
    }
  }
}
