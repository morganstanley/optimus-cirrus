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
package ch.qos.logback.classic.pattern {
  sealed abstract class NamedConverterExtension extends ClassOfCallerConverter {
    protected final def getAbbreviator = abbreviator
  }
}

package optimus.logging {

  import java.util.concurrent.ConcurrentHashMap

  import ch.qos.logback.classic.PatternLayout
  import ch.qos.logback.classic.pattern.NamedConverterExtension
  import ch.qos.logback.classic.spi.ILoggingEvent

  object OptimusLogging {
    lazy val installed = {
      PatternLayout.DEFAULT_CONVERTER_MAP.put("C", classOf[CachedNamedConverter].getName)
      PatternLayout.DEFAULT_CONVERTER_MAP.put("class", classOf[CachedNamedConverter].getName)

      PatternLayout.DEFAULT_CONVERTER_MAP.put("lo", classOf[CachedLoggerConverter].getName)
      PatternLayout.DEFAULT_CONVERTER_MAP.put("logger", classOf[CachedLoggerConverter].getName)
      PatternLayout.DEFAULT_CONVERTER_MAP.put("c", classOf[CachedLoggerConverter].getName)

      PatternLayout.DEFAULT_CONVERTER_MAP.put("d", classOf[DateConverter].getName)
      PatternLayout.DEFAULT_CONVERTER_MAP.put("date", classOf[DateConverter].getName)

      // we could also add some stuff about logging optimus aware stuff here
      true
    }

    def install(): Unit = installed
  }

  class CachedNamedConverter extends NamedConverterExtension {

    var cachedNames: ConcurrentHashMap[String, String] = null

    override def start(): Unit = {
      super.start()
      if (getAbbreviator ne null) {
        cachedNames = new ConcurrentHashMap[String, String]
      }
    }
    override def convert(event: ILoggingEvent): String = {
      // Note - After Java 11 migration this can be improved
      // to walk the stack
      val fqn: String = getFullyQualifiedName(event)
      val abbreviator = getAbbreviator

      if (abbreviator eq null) fqn
      // the default abbreviator generates loads of garbage, so cache the responses
      else {
        val exists = cachedNames.get(fqn)
        if (exists ne null) exists
        else cachedNames.computeIfAbsent(fqn, abbreviator.abbreviate)
      }
    }
  }
  class CachedLoggerConverter extends CachedNamedConverter {
    override protected def getFullyQualifiedName(event: ILoggingEvent): String = event.getLoggerName
  }
}
