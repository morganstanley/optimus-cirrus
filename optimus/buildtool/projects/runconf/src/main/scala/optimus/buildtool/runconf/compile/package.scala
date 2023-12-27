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
package optimus.buildtool.runconf

import optimus.buildtool.runconf.plugins.EnvInternal
import optimus.scalacompat.collection._

package object compile {
  type Block = String
  private[compile] type RawProperties = Map[String, Any]
  type Endo[T] = T => T
  val T: Types.type = Types

  private[compile] def envInternalToExternal(env: EnvInternal): Map[String, String] = {
    env.mapValuesNow {
      case Left(single) => single
      case Right(seq)   => seq.mkString(" ")
    }.toMap
  }
}
