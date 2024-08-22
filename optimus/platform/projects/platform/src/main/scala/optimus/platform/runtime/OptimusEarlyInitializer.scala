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
import optimus.platform.AdvancedUtils
import org.slf4j.LoggerFactory

import java.util.ServiceLoader
import scala.util.control.NonFatal

/**
 * OptimusEarlyInitializer will be called very early in the process before most optimus initialization This allows an
 * application register a list of initialization steps that must occur earlier than it would otherwise be able to
 * execute. Initializers should be a class extending the OptimusEarlyInitializer trait with a no-args constructor
 *
 * To register an initializer, edit/create a ServiceLoader META-INF/services file
 */
object OptimusEarlyInitializer {
  private val log = LoggerFactory.getLogger(getClass)

  lazy val ensureInitialized: Unit = {
    ServiceLoader
      .load(classOf[OptimusEarlyInitializer])
      .forEach(i =>
        try {
          log.info(s"OptimusEarlyInitializer: initializing $i")
          val (time, _) = AdvancedUtils.timed {
            i.init()
          }
          log.info(s"OptimusEarlyInitializer: $i completed in ${time / 1e6} ms")
        } catch {
          case NonFatal(e) => log.error(s"Error running $i", e)
        })
  }
}

trait OptimusEarlyInitializer {
  def init(): Unit
}
