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
package optimus.buildtool.config

final case class OctalMode private (mode: Int) {
  // mode is the numeric representation (e.g., 511), modeString is the string representation (e.g., "777")
  require(OctalMode.isSupportedMode(mode), s"Invalid mode $modeString")

  def modeString: String = mode.toOctalString
  override def toString: String = s"OctalMode($modeString)"
}

object OctalMode {
  private def toModeInt(modeString: String) = Integer.parseInt(modeString, 8)
  private val MaxSupportedMode = toModeInt("777")

  private def isSupportedMode(mode: Int): Boolean =
    0 <= mode && mode <= MaxSupportedMode

  final val Default = OctalMode.fromModeString("644") // u=rw,go=r
  final val Execute = OctalMode.fromModeString("755") // u=rwx,go=rx

  def fromModeString(mode: String): OctalMode = fromMode(toModeInt(mode))
  def fromMode(mode: Int): OctalMode = new OctalMode(mode)
}
