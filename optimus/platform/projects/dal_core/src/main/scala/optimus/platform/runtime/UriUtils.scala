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
package optimus.platform.runtime

import java.net.URI

import scala.util.Try

object UriUtils {

  def getQueryMap(uri: URI): Map[String, String] = {
    Option(uri.getQuery) getOrElse ("") split "&" map { _ split "=" } flatMap { kv =>
      if (kv.length == 2)
        Some(kv(0), kv(1))
      else
        None
    } toMap
  }

  def appendToQuery(originalUri: String, key: String, value: String): String =
    updateQuery(originalUri, addQueryPartAndBuildUriQuery(key, value))

  def appendToQuery(originalUri: String, newQueryMap: Map[String, String]): String =
    updateQuery(originalUri, addQueryPartAndBuildUriQuery(newQueryMap))

  def removeFromQuery(originalUri: String, key: String): String =
    updateQuery(originalUri, removeQueryPartAndBuildUriQuery(key))

  def queryContainsBooleanKey(originalUri: String, key: String): Boolean =
    getQueryMap(new URI(originalUri)).get(key) exists { b =>
      Try(b.toBoolean).getOrElse(false)
    }

  def getDoubleValueFromUriQuery(originalUri: String, key: String): Option[Double] = {
    getQueryMap(new URI(originalUri)).get(key).map(b => Try(b.toDouble).getOrElse(0.0d))
  }

  def matchesAuthority(uri: String, expectedAuthority: String): Boolean = {
    Try(expectedAuthority == new URI(uri).getAuthority).getOrElse(false)
  }

  def buildUriQuery(m: Map[String, String]): String = m map { case (k, v) => s"${k}=${v}" } mkString "&"

  private def updateQuery(originalUri: String, f: Map[String, String] => String): String = {
    val u = new URI(originalUri)
    val updatedQuery = f(getQueryMap(u))
    new URI(
      u.getScheme,
      u.getAuthority,
      u.getPath,
      if (updatedQuery.isEmpty) null else updatedQuery,
      u.getFragment).toString
  }

  private def addQueryPartAndBuildUriQuery(key: String, value: String)(originalQueryMap: Map[String, String]): String =
    buildUriQuery(originalQueryMap + (key -> value))

  private def addQueryPartAndBuildUriQuery(neqQueryMap: Map[String, String])(
      originalQueryMap: Map[String, String]): String =
    buildUriQuery(originalQueryMap ++ neqQueryMap)

  private def removeQueryPartAndBuildUriQuery(key: String)(originalQueryMap: Map[String, String]): String =
    buildUriQuery(originalQueryMap - key)
}
