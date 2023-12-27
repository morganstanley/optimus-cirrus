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

import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.platform.util.Log
import sbt.internal.inc.AnalyzingCompiler
import sbt.internal.inc.CompileFailed
import sbt.internal.inc.RawCompiler
import sbt.internal.inc.ScalaInstance
import xsbti.compile.ClasspathOptionsUtil

object ZincInterface extends Log {

  def getInterfaceJar(
      interfaceDir: Directory,
      scalaInstance: => ScalaInstance,
      zincClasspath: => Seq[JarAsset],
      zincVersion: String,
      onStartCompilation: => Unit
  ): JarAsset = synchronized {
    // the interface classes vary if they are compiled with a different ScalaC version and if they were compiled from
    // different Zinc sources so we include both versions in the cached file name.
    val targetJar =
      interfaceDir.resolveJar(s"compilerInterface-scala-${scalaInstance.actualVersion}-zinc-$zincVersion.jar")

    if (!targetJar.existsUnsafe) {
      onStartCompilation
      val zincLogger = new ZincGeneralLogger
      val rawCompiler = new RawCompiler(scalaInstance, ClasspathOptionsUtil.auto, zincLogger)
      val sourceJars = zincClasspath.filter(_.name.endsWith("-sources.jar"))
      val interfaceJars = zincClasspath.filter(_.name.contains("interface"))
      if (sourceJars.isEmpty)
        throw new RuntimeException(s"Missing source jars for compiler interface while building $targetJar")
      if (interfaceJars.isEmpty)
        throw new RuntimeException(s"Missing interface jars for compiler interface while building $targetJar")

      log.debug(s"Building $targetJar from $sourceJars, $interfaceJars")

      def runCompilation(): Unit = AnalyzingCompiler.compileSources(
        sourceJars.map(_.path),
        targetJar.path,
        interfaceJars.map(_.path),
        targetJar.name,
        rawCompiler,
        new ZincGeneralLogger
      )

      System.clearProperty("scala.home")
      try runCompilation()
      catch {
        // This is workaround for problem with temp dirs on Jenkins
        case e: CompileFailed if e.toString.startsWith("Error compiling sbt component") =>
          println(
            s"Error compiling sbt component, retrying. " +
              s"${e.getClass.getSimpleName}: ${e.getMessage}]( sbt components, retrying.")
          runCompilation()
      }
    }

    targetJar
  }
}
