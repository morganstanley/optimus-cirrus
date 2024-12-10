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

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.layout.TTLLLayout
import ch.qos.logback.classic.spi.Configurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.spi.ContextAwareBase
// import msjava.logbackutils.async.AsyncAppender
// import msjava.logbackutils.async.AsyncDispatcherQueueImplPatch

/**
 * Similar to default configuration of logback, except that console appender is wrapped in async appender.
 *
 * @see
 *   ch.qos.logback.classic.BasicConfigurator
 */
class DefaultAsyncLogbackConfigurator extends ContextAwareBase with Configurator {

  override def configure(lc: LoggerContext) = ??? /* {
    addInfo("Setting up default configuration for async logging.")

    val ca = new ConsoleAppender[ILoggingEvent]
    ca.setContext(lc)
    ca.setName("SYNC_CONSOLE")
    val encoder = new LayoutWrappingEncoder[ILoggingEvent]
    encoder.setContext(lc)

    // same as
    // PatternLayout layout = new PatternLayout();
    // layout.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
    val layout = new TTLLLayout

    layout.setContext(lc)
    layout.start()
    encoder.setLayout(layout)

    ca.setEncoder(encoder)
    ca.start()

    // config async appender
    val asyncAppender = new AsyncAppender[ILoggingEvent]
    asyncAppender.setContext(lc)
    asyncAppender.setName("CONSOLE")
    val dispatcher = new AsyncDispatcherQueueImplPatch[ILoggingEvent]
    dispatcher.setContext(lc)
    asyncAppender.setDispatcher(dispatcher)
    asyncAppender.addAppender(ca)
    asyncAppender.start()

    val rootLogger = lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    // rootLogger.addAppender(asyncAppender)
  } */
}
