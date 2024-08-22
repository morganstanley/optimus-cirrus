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

import java.awt.datatransfer._
import java.awt.Toolkit

object SClipboard {
  val supportedFlavors =
    Array(new DataFlavor("text/html;class=java.lang.String"), new DataFlavor("text/plain;class=java.lang.String"))

  def copy(html: String, text: String = "no text"): Unit = {
    val cb = Toolkit.getDefaultToolkit.getSystemClipboard
    cb.setContents(new SimpleTransferable(html, text), null)
  }
}

class SimpleTransferable(val html: String, val text: String) extends Transferable {

  override def getTransferDataFlavors: Array[DataFlavor] = SClipboard.supportedFlavors
  override def isDataFlavorSupported(flavor: DataFlavor): Boolean = SClipboard.supportedFlavors.contains(flavor)
  override def getTransferData(flavor: DataFlavor): String = {
    if (flavor == SClipboard.supportedFlavors(0)) html
    else if (flavor == SClipboard.supportedFlavors(1)) text
    else throw new UnsupportedFlavorException(flavor)
  }
}
