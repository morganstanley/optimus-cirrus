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
package optimus.graph

import optimus.breadcrumbs.ChainedID;

trait AwaitableContext;

trait Awaitable {
  def getId: Int
  def ID: ChainedID
  def toSimpleName: String
  def getAppId: Option[String]
  def tagString: Option[String]

  def flameFrameName(extraMods: String): String

  def launch(awaitableContext: AwaitableContext): Unit
  def getProfileId: Int

  // Lose a bit of resolution to keep IDs prettier and avoid having to deal with dashes
  def stackId: Long = Math.abs(getLauncherStackHash)
  def getLauncherStackHash: Long
  def getLauncher: Awaitable

  def setLaunchData(awaitable: Awaitable, hash: Long, implFlags: Int): Unit

  def elideChildFrame: Boolean

  def underlyingAwaitable: Awaitable

}
