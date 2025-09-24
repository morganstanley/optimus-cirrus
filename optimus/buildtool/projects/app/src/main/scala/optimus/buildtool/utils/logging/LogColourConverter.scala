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
package optimus.buildtool.utils.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.pattern.color.ANSIConstants
import ch.qos.logback.core.pattern.color.ForegroundCompositeConverterBase
import optimus.buildtool.utils.Utils

class LogColourConverter extends ForegroundCompositeConverterBase[ILoggingEvent] {
  override def getForegroundColorCode(event: ILoggingEvent): String = {
    if (event.getLoggerName == Utils.SuccessLog.javaLogger.getName)
      ANSIConstants.BOLD + ANSIConstants.GREEN_FG
    else if (event.getLoggerName == Utils.WarningLog.javaLogger.getName)
      ANSIConstants.YELLOW_FG
    else if (event.getLoggerName == Utils.FailureLog.javaLogger.getName)
      ANSIConstants.BOLD + ANSIConstants.RED_FG
    else
      ANSIConstants.DEFAULT_FG
  }
}
