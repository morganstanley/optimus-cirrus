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

import java.io.File
import java.math.BigInteger
import java.security.MessageDigest

import optimus.platform.relational.RelationalException
import optimus.platform.PackageAliases

import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.VirtualDirectory
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Global, Settings}

class RuntimeScalaCompiler(
    targetDir: Option[File] = None,
    fatalWarnings: Boolean = true,
    pluginOptions: Seq[String] = Seq.empty,
    val parentClassLoaderOpt: Option[ClassLoader] = None) {

  import scala.reflect.internal.util.AbstractFileClassLoader

  val target = targetDir match {
    case Some(dir) => AbstractFile.getDirectory(dir)
    case None      => new VirtualDirectory("(memory)", None)
  }

  private val settings = new Settings()
  settings.deprecation.value = true
  settings.unchecked.value = true
  settings.outputDirs.setSingleOutput(target)
  settings.usejavacp.value = true
  settings.async.value = true

  settings.require.value ++= List("entity")
  settings.plugin.value ++= Seq(ScalaCompilerUtils.entityJars)

  settings.bootclasspath.append(ScalaCompilerUtils.classpath)
  settings.classpath.append(ScalaCompilerUtils.classpath)
  settings.feature.value = true
  settings.fatalWarnings.value = fatalWarnings

  settings.pluginOptions.value ++= pluginOptions

  // settings.YaliasPackage.tryToSet(PackageAliases.aliases)

  val reporter = new StoreReporter(settings)
  val global = new Global(settings, reporter)

  val classLoader = new AbstractFileClassLoader(target, parentClassLoaderOpt.getOrElse(this.getClass.getClassLoader))

  def compile(str: String): Class[_] = {
    val className = classNameForCode(str)
    findClass(className).getOrElse {
      global.reporter.reset()
      val run = new global.Run
      val sourceFiles = List(new BatchSourceFile("(inline)", wrapCodeInClass(className, str)))
      run.compileSources(sourceFiles)

      findClass(className).getOrElse {
        val error = reporter.infos.filter(info => info.severity == reporter.ERROR).mkString("\n")
        throw new RelationalException(s"$error\nCan't compile code\n$str")
      }
    }
  }

  def compileEntity(str: String, entityName: String): Class[_] = {
    val className = classNameForCode(str)
    findClass(className).getOrElse {
      global.reporter.reset()
      val run = new global.Run
      val actualCode = wrapCodeInClassForEntity(className, entityName, str)
      val sourceFiles = List(new BatchSourceFile("(inline)", actualCode))
      run.compileSources(sourceFiles)

      findClass(className).getOrElse {
        val error = reporter.infos.filter(info => info.severity == reporter.ERROR).mkString("\n")
        throw new RelationalException(s"$error\nCan't compile code\n$actualCode")
      }
    }
  }

  def findClass(className: String): Option[Class[_]] = {
    try {
      val cls = classLoader.loadClass(className)
      Some(cls)
    } catch {
      case e: ClassNotFoundException => None
    }
  }

  def classNameForCode(code: String): String = {
    val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
    s"sha${new BigInteger(1, digest).toString(16)}"
  }

  private def wrapCodeInClass(className: String, code: String) = {
    s"class $className extends (() => Any) {\n  def apply() = {\n$code\n  }\n}\n"
  }

  private def wrapCodeInClassForEntity(className: String, entityName: String, code: String) = {
    s"class $className extends (() => Any) {\n $code\n def apply() = {$entityName()}\n}\n"
  }
}
