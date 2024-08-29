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
package optimus.platform.dal.config

import java.io.File
import java.nio.charset.CodingErrorAction
import java.nio.file.Path
import scala.io.Codec
import scala.io.Source

final case class DalGoto(app: String, env: String, alias: String)(implicit defaultDataSource: DalGoto.DataSource) {
  def aliasURL: String = s"http://$alias/"
  def rawURL: Option[String] = DalGotoUrlLoader.default.getRawURL(alias)
}

object DalGoto {
  trait DataSource {
    def path: Path
  }

  def fetchGotoUrl(app: String, env: String, gotoFile: String, endpoint: String)(implicit
      defaultDataSource: DataSource): String = {
    val urlAlias: String = DalGoto(app, env).alias
    new DalGotoUrlLoader(new File(gotoFile))
      .getRawURL(urlAlias)
      .getOrElse({
        throw new IllegalStateException(s"$urlAlias is not set in goto. Please check via http://gotogui/")
      })
      .split("/index.html")(0) + endpoint
  }

  def apply(app: String, env: String)(implicit defaultDataSource: DataSource): DalGoto = {
    val alias = (app, env) match {
      case ("dalviewer", mode)      => s"dalviewer-$mode"
      case ("dalusage", "prod2")    => "dalusage"
      case ("dalportal", mode)      => s"dalportal-$mode"
      case ("hwinventory", mode)    => s"hwinventory-$mode"
      case ("dalshardconfig", mode) => s"dalshardconfig-$mode"
      case (app, mode)              => s"$app$mode"
    }
    DalGoto(app, env, alias)
  }
}

object DalGotoUrlLoader {
  def default(implicit defaultDataSource: DalGoto.DataSource): DalGotoUrlLoader = {
    new DalGotoUrlLoader(defaultDataSource.path.toFile)
  }
}

class DalGotoUrlLoader(gotoFile: File) {
  private val pattern = "(.*)\\s(.*)".r
  private lazy val all: Map[String, String] =
    Source
      .fromFile(gotoFile)(
        Codec.ISO8859.onMalformedInput(CodingErrorAction.IGNORE).onUnmappableCharacter(CodingErrorAction.IGNORE))
      .getLines()
      .filter(line => line.startsWith("dal") || line.startsWith("entityviewer") || line.startsWith("hwinventory"))
      .flatMap {
        case pattern(alias, url) => Some(alias, url)
        case _                   => None
      }
      .toMap
  private[optimus] def getRawURL(alias: String): Option[String] = all.get(alias)
}

object OS {
  def isWindows: Boolean = System.getProperty("os.name").toLowerCase.contains("windows")
}
