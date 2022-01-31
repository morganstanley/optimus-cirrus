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
package optimus.breadcrumbs
package auth

import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * A service class to initialize authentication mechanisms for a breadcrumbs connection.
 * By default, does nothing at all.
 */
class CrumbsAuthProvider {
  def initAuth(props: ju.Map[String, AnyRef]) {}
}

object CrumbsAuthProvider {
  private val instance = {
    val loaded = ju.ServiceLoader.load(classOf[CrumbsAuthProvider]).iterator().asScala.toList
    loaded match {
      case p :: Nil => p // one was provided; use it
      case Nil      => new CrumbsAuthProvider // nothing was provided; use the no-op version
      case _ =>
        throw new IllegalArgumentException(s"Multiple ${classOf[CrumbsAuthProvider].getName} provided: [$loaded]")
    }
  }
  implicit def asInstance(thiz: CrumbsAuthProvider.type): CrumbsAuthProvider = // yes we're totally a singleton why do you ask
    instance
}
