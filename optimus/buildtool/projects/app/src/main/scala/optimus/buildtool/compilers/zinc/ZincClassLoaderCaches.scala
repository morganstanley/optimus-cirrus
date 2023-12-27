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
package optimus.buildtool.compilers.zinc

import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.ConcurrentHashMap

import sbt.internal.inc.classpath.ClassLoaderCache

private[buildtool] object ZincClassLoaderCaches extends ZincClassLoaderCaches
private[buildtool] class ZincClassLoaderCaches {
  lazy val classLoaderCache = Some(new ClassLoaderCache(null: ClassLoader))

  private val classLoaders: ConcurrentHashMap[Seq[File], ClassLoader] = new ConcurrentHashMap
  def classLoaderFor(scalaClasspath: Seq[File]): ClassLoader =
    classLoaders.computeIfAbsent(
      scalaClasspath,
      // pass null so it's not parented to the current classloader, because we want isolation
      cp => new URLClassLoader(scalaClasspath.map(_.toURI.toURL).toArray, null)
    )
}
