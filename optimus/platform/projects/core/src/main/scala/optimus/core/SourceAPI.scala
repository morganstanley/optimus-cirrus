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
package optimus.core

import msjava.slf4jutils.scalalog.Logger

// API for source code specific features
trait SourceAPI {
  def doNotSubmit[A](a: A): A = a

  private[core] class DoNotSubmitForLogger(val l: Logger) {
    def doNotSubmit: Logger = l
  }

  implicit def doNotSubmitLogDecorator(l: Logger): DoNotSubmitForLogger = new DoNotSubmitForLogger(l)
}
