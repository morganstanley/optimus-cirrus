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
package optimus.platform

import java.io.File

object ChromeBrowserConfiguration {

  /**
   * Due to the fact that the findChromeLocation method can be accessed in a delayedInit call from runUI in chrome mode
   * for OptimusUiAppTrait, all the vals it depends on in this object need to be lazy, otherwise will be null at runtime
   * even if they are just String constants. Do not make them normal vals or they will fail silently for some.
   */

  private lazy val SystemPropertyName = "optimus.ui.chromeExeLocation"
  lazy val chromeExeLocation: String = System.getProperty(SystemPropertyName, findChromeLocation)

  private lazy val defaultWindows64BitsLocation = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
  private lazy val defaultWindows32BitsLocation = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
  private lazy val defaultLinuxLocation = "/usr/bin/chromium-browser"

  def findChromeLocation: String = {
    if (new File(defaultWindows64BitsLocation).isFile) {
      defaultWindows64BitsLocation
    } else if (new File(defaultWindows32BitsLocation).isFile) {
      defaultWindows32BitsLocation
    } else if (new File(defaultLinuxLocation).isFile) {
      defaultLinuxLocation
    } else {
      throw new UnsupportedOperationException(
        s"Chrome executable not found in its default 64 or 32 bit locations. Please specify a custom location with -D$SystemPropertyName")
    }
  }
}
