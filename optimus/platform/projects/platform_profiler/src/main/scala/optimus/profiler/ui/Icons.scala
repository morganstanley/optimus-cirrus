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
package optimus.profiler.ui
import optimus.platform_profiler.config.StaticConfig

import java.awt.Image
import java.awt.Toolkit
import java.net.URL
import javax.swing.ImageIcon

object Icons {

  private val iconsDir = StaticConfig.string("iconsDir")

  private def getIconURL(filename: String): URL =
    getClass.getResource(s"$iconsDir/$filename")

  private def getIcon(filename: String): ImageIcon =
    new ImageIcon(getIconURL(filename))

  val home: ImageIcon = getIcon("home.png")
  val syncPoint: ImageIcon = getIcon("sync.png")
  val syncBranch: ImageIcon = getIcon("branch.png")
  val entryPoint: ImageIcon = getIcon("entry.png")

  val dump: ImageIcon = getIcon("dump.png")
  val reset: ImageIcon = getIcon("reset.png")
  val refresh: ImageIcon = getIcon("refresh.png")

  val expand: ImageIcon = getIcon("ui_plus.png")
  val collapse: ImageIcon = getIcon("ui_minus.png")

  val insect: Image = Toolkit.getDefaultToolkit.getImage(getIconURL("insect-icon.png"))

  val callee: ImageIcon = getIcon("callee.png")
  val caller: ImageIcon = getIcon("caller.png")
  val uiInvalidates: ImageIcon = getIcon("ui_invalidates.png")
  val commonParent: ImageIcon = getIcon("common_parent.png")

  val folderOpen: ImageIcon = getIcon("groupOpen.png")
  val folderClosed: ImageIcon = getIcon("groupClosed.png")
  val invalidatingTweak: ImageIcon = getIcon("tweakInvalidation.png")
}
