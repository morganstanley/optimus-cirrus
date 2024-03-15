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

import msjava.slf4jutils.scalalog._

trait Log {
  private[util] val logImpl = new LogWrapper(this.getClass)
  protected def log: Logger = logImpl.log
}
private[util] class LogWrapper(clazz: Class[_]) extends Serializable {
  @transient private[util] val log = getLogger(clazz)
  private def readResolve: AnyRef = {
    new LogWrapper(clazz)
  }
}
