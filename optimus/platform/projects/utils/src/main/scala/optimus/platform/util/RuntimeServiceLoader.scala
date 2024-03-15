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

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/*
 * from ServiceLoader's documentation
 *
 * A service provider is identified by placing a provider-configuration file
 * in the resource directory META-INF/services. The file's name is
 * the fully-qualified binary name of the service's type. The file contains
 * a list of fully-qualified binary names of concrete provider classes, one per line.
 * Space and tab characters surrounding each name, as well as blank lines,
 * are ignored. The comment character is '#' ('\u0023', NUMBER SIGN);
 * on each line all characters following the first comment character are ignored.
 * The file must be encoded in UTF-8.
 */
abstract class RuntimeServiceLoader[T: Manifest] extends ServiceLoaderUtils[T] {
  protected val service: T = loadServices() match {
    case List(s) => s
    case services =>
      val errorMsg =
        s"Expected one ${clazz.getSimpleName}, found ${services.size} - $services - classpath = ${System.getProperty("java.class.path")}"
      throw new IllegalArgumentException(errorMsg)
  }
}

/*
 * Similar to RuntimeServiceLoader. It retrieves at runtime all services that extend a given T, rather than just one
 */
abstract class RuntimeServicesLoader[T: Manifest] extends ServiceLoaderUtils[T] {
  protected val services: List[T] = loadServices()
}

class ServiceLoaderUtils[T: Manifest] {

  protected val clazz: Class[T] = implicitly[Manifest[T]].runtimeClass.asInstanceOf[Class[T]]

  protected def loadServices(): List[T] =
    ServiceLoader.load(clazz, classOf[RuntimeServiceLoader[T]].getClassLoader).iterator.asScala.toList
}

object ServiceLoaderUtils {
  def all[T: Manifest]: Seq[T] = {
    object loader extends ServiceLoaderUtils[T] {
      val services: List[T] = loadServices()
    }
    loader.services
  }
}
