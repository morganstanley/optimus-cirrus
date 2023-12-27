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

import java.nio.file.Path
import java.nio.file.Paths

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.utils.Utils

object ZincInstallationLocator {
  private val log = getLogger(getClass)

  def inferZincPathAndVersion(over: Option[String] = None): (Path, String) = {
    // Check explicit override...
    (over orElse Option(System.getProperty("buildtool.zinc.home.version")))
      .map { hv =>
        val idx = hv.lastIndexOf(':')
        val h = hv.substring(0, idx)
        val v = hv.substring(idx + 1)
        log.info(s"Using overridden zincPath and zincVersion: $h $v")
        (Paths.get(h), v)
      }
      .getOrElse {
        // ... otherwise try to deduce zinc install location from classpath
        // We'll look for compiler-interface, since it doesn't have a scala version in its name, eg.
        // /afs/path/to/msde/PROJ/zinc/2019.07.19-2-1.3.x-ms/zinc/org.scala-sbt/compiler-interface/compiler-interface-2019.07.19-2-1.3.x-ms.jar
        val jarPath = Utils.findJar(classOf[sbt.internal.inc.InvalidationProfiler])
        // Should look like
        //   /afs/path/to/msde/PROJ/zinc/2019.08.01-1-1.3.x-ms/zinc/org.scala-sbt/zinc-core_2.12/zinc-core_2.12-2019.08.01-1-1.3.x-ms.jar
        // We want -----------------------------------------^
        assert(jarPath.getNameCount > 3, s"$jarPath does not look like a zinc path")
        val libDir = jarPath.getParent
        val sbtDir = libDir.getParent
        val zincDir = sbtDir.getParent
        assert(zincDir.getFileName.toString == "zinc", s"$jarPath should contain a 'zinc' directory")
        val lib = libDir.getFileName.toString
        val pat = s"$lib-(.+)\\.jar".r
        val version = jarPath.getFileName.toString match {
          case pat(v) => v
          case _      => throw new AssertionError(s"Can't extract version from $jarPath")
        }

        log.info(s"Inferring zincPath and zincVersion: $zincDir, $version")
        (zincDir, version)
      }
  }

}
