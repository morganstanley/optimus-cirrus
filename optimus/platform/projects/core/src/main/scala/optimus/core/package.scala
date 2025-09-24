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
package optimus
import msjava.slf4jutils.scalalog._

package object core {
  private[optimus] val log = getLogger("optimus.graph")

  def needsPlugin: Nothing = throw new NoSuchMethodException(
    "This source file needs to be compiled with optimus plugin")

  /**
   * for use with @alwaysAutoAsyncArgs.  Use this variant of needsPlugin to indicate usage pattern
   */
  def needsPluginAlwaysAutoAsyncArgs: Nothing = throw new NoSuchMethodException(
    "This source file needs to be compiled with optimus plugin (alwaysAutoAsyncArgs)")

  /**
   * for use with @withLocationTag.  Use this variant of needsPlugin to indicate usage pattern
   */
  def needsPluginWithLocationTag: Nothing = throw new NoSuchMethodException(
    "This source file needs to be compiled with optimus plugin (withLocationTag)")

}
