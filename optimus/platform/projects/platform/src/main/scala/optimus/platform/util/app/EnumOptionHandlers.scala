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
package optimus.platform.util.app

import optimus.core.utils.RuntimeMirror
import optimus.platform.util.Enum
import optimus.utils.app.CommaDelimitedArgumentOptionHandler
import optimus.utils.app.OptionOptionHandler
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

import scala.util.Try

object EnumOptionHandler {
  type EnumClass = EnumOptionClass
}
final class EnumOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Any])
    extends OneArgumentOptionHandler[Any](parser, option, setter) {
  override def parse(argument: String): Any = {
    val companion = Option(setter.asAnnotatedElement.getAnnotation(classOf[EnumOptionClass]))
      .flatMap { a =>
        resolveCompanion(a.clazz)
      }
      .orElse {
        resolveCompanion(setter.getType)
      }
      .get

    companion.withName(argument)
  }

  def resolveCompanion(clazz: Class[_]): Option[Enum[_]] = Try {
    val rm = RuntimeMirror.forClass(this.getClass)
    val companionModule = rm.classSymbol(clazz).companion.asModule
    rm.reflectModule(companionModule).instance
  }.toOption.collect { case e: Enum[_] => e }
}

object EnumOptionOptionHandler {
  type EnumClass = EnumOptionClass
}
final class EnumOptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[Any]])
    extends OptionOptionHandler[Any](parser, option, setter) {
  override def convert(s: String): Any = new EnumOptionHandler(parser, option, setter.asInstanceOf[Setter[Any]])
    .parse(s)
}

object DelimitedEnumOptionHandler {
  type EnumClass = EnumOptionClass
}
final class DelimitedEnumOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Seq[Any]])
    extends CommaDelimitedArgumentOptionHandler[Any](parser, option, setter) {
  def convert(s: String): Any = new EnumOptionHandler(parser, option, setter.asInstanceOf[Setter[Any]])
    .parse(s)
}
