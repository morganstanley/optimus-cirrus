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
import java.lang.{Boolean => JavaBoolean}
import java.lang.{Double => JavaDouble}
import java.lang.{Short => JavaShort}
import java.lang.{Integer => JavaInteger}
import java.lang.{Long => JavaLong}
import java.lang.{Float => JavaFloat}
import java.lang.{Character => JavaCharacter}
import java.lang.{Byte => JavaByte}
import java.net.URI
import java.net.URL
import java.net.URLClassLoader
import java.net.URLDecoder
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import msjava.slf4jutils.scalalog._
import optimus.config.spray.json.JsValue
import optimus.core.utils.RuntimeMirror
import optimus.graph.{Settings => GraphSettings}
import optimus.platform.metadatas.internal.EntityBaseMetaData
import optimus.platform.relational.RelationalException
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.utils.ClassPathUtils
import optimus.scalacompat.collection.CanEqual
import optimus.tools.scalacplugins.entity.EntityPlugin
import optimus.tools.scalacplugins.entity.OptimusPhaseInfo
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter
import org.objectweb.asm.Type.{SHORT => AsmShort}
import org.objectweb.asm.Type.{FLOAT => AsmFloat}
import org.objectweb.asm.Type.{CHAR => AsmChar}
import org.objectweb.asm.Type.{DOUBLE => AsmDouble}
import org.objectweb.asm.Type.{INT => AsmInt}
import org.objectweb.asm.Type.{BYTE => AsmByte}
import org.objectweb.asm.Type.{BOOLEAN => AsmBoolean}
import org.objectweb.asm.Type.{LONG => AsmLong}
import org.objectweb.asm.Type.{OBJECT => AsmObject}
import org.objectweb.asm.{Type => AsmType}
import com.typesafe.config.ConfigFactory

import scala.reflect.runtime.universe._
import scala.tools.nsc.plugins.Plugin
import scala.tools.reflect.ToolBoxError
import optimus.platform.util.xml.XmlLoader

object ScalaCompilerUtils {
  private val log = getLogger[ScalaCompilerUtils.type]

  // we hold this as a lazy val because that way if it fails to init (e.g. due to some external classloading problems)
  // then the initialization will be retried on next access (c.f. a non lazy val, which would cause this object
  // to fail to initialize completely)
  private lazy val toolboxMgr = new ScalaToolboxManager
  lazy val compiler = new RuntimeScalaCompiler(entityJars, Seq("entity"))

  val isURLClassLoader: Boolean = {
    getClass().getClassLoader() match {
      case urlLoader: URLClassLoader => true
      case _                         => false
    }
  }
  val stringCompiler = System.getProperty("optimus.priql.stringCompiler", if (isURLClassLoader) "global" else "toolbox")

  def eval[T](tree: Tree): T =
    try {
      toolboxMgr.eval[T](tree)
    } catch {
      case e: ToolBoxError =>
        // so... sometimes the compiler seems to get in a bad state from previous (successful) compilations, and
        // resetting it makes it work again...
        toolboxMgr.resetToolbox()
        log.warn("Toolbox compilation failed. Will try resetting Toolbox and recompiling...")
        val r = toolboxMgr.eval[T](tree)
        log.warn("Toolbox compilation succeeded on retry")
        r
    }

  def eval[T](source: String) = {
    // -Doptimus.priql.stringCompiler=global allows use nsc.Global to compile code
    val compiler = stringCompiler.toLowerCase()
    log.debug(s"Compile code using ${compiler}")
    compiler match {
      case "toolbox" => toolboxEval[T](source)
      case "global"  => compilerEval[T](source)
      case _ => throw new RelationalException("Please set 'optimus.priql.stringCompiler' to 'toolbox' or 'global'.")
    }
  }

  def parse(str: String): Tree =
    try {
      toolboxMgr.parse(str)
    } catch {
      case e: ToolBoxError =>
        // so... sometimes the compiler seems to get in a bad state from previous (successful) compilations, and
        // resetting it makes it work again...
        toolboxMgr.resetToolbox()
        log.warn("Toolbox parsing failed. Will try resetting Toolbox and reparsing...")
        val r = toolboxMgr.parse(str)
        log.warn("Toolbox parsing succeeded on retry")
        r
    }

  def toolboxEval[T](str: String): T =
    try {
      toolboxMgr.eval[T](str)
    } catch {
      case e: ToolBoxError =>
        // so... sometimes the compiler seems to get in a bad state from previous (successful) compilations, and
        // resetting it makes it work again...
        toolboxMgr.resetToolbox()
        log.warn("Toolbox compilation failed. Will try resetting Toolbox and recompiling...")
        val r = toolboxMgr.eval[T](str)
        log.warn("Toolbox compilation succeeded on retry")
        r
    }

  def compilerEval[T](str: String) = {
    val cls = compiler.compile(str)
    cls.getConstructor().newInstance().asInstanceOf[() => Any].apply().asInstanceOf[T]
  }

  lazy val entityJarSeq: Seq[String] = {
    val entries = Seq(
      "entityplugin" -> classOf[EntityPlugin],
      "stagingplugin" -> classOf[OptimusPluginReporter],
      "utils" -> classOf[EntityBaseMetaData],
      "alarms" -> classOf[OptimusPhaseInfo],
      "spray_json" -> classOf[JsValue],
      "scala_compat" -> classOf[CanEqual], // java jar
      "scala_compat" -> Class.forName("optimus.scalacompat.collection.package$"), // scala jar
      "typesafeconfig" -> classOf[ConfigFactory]
    ).map { case (name, clazz) =>
      Option(GraphSettings.pluginPathFromLibPath(GraphSettings.entityPluginPath, name))
        .orElse(findPathJar(clazz))
        .getOrElse(findIDEBuildPluginEntry(name))
    }
    val resources = pluginResources("entity")
    val allEntries = (resources ++ entries).distinct
    log.info(s"Found entity plugin jar path: ${allEntries.mkString(", ")}")
    allEntries
  }

  lazy val entityJars = entityJarSeq.mkString(File.pathSeparator)

  private def pluginResources(pluginName: String): Seq[String] = {
    val resources = getClass.getClassLoader.getResources(Plugin.PluginXML)
    import scala.jdk.CollectionConverters._
    // there can be multiple plugins on the classpath (e.g. staging, entity) so we need to find the right one
    resources.asScala
      .find(r => (XmlLoader.load(r) \ "name").text == pluginName)
      .map(path => extractResourcePath(path).toString)
      .toSeq
  }

  def extractResourcePath(resourceURL: URL): Path = {
    resourceURL.getProtocol match {
      case "jar" =>
        val archivePath = resourceURL.getPath.split("!").head
        uriToPath(new URI(archivePath))
      case _ => Paths.get(resourceURL.toURI).getParent
    }
  }

  private def uriToPath(uri: URI): Path = {
    val schemePart = uri.getSchemeSpecificPart
    Option(uri.getAuthority) match {
      case None if schemePart.startsWith("/") => Paths.get(uri)
      case _ =>
        if (!schemePart.startsWith("/") && schemePart.contains(":")) Paths.get("//" + schemePart)
        else Paths.get(schemePart)
    }
  }

  private def findPathJar(context: Class[_]): Option[String] = {
    val rawName = context.getName()
    val classFileName = {
      /* rawName is something like package.name.ContainingClass$ClassName. We need to turn this into ContainingClass$ClassName.class. */
      val idx = rawName.lastIndexOf('.')
      if (idx == -1) rawName else s"${rawName.substring(idx + 1)}.class"
    }

    val uri = context.getResource(classFileName).toString()
    if (uri.startsWith("file:") || !uri.startsWith("jar:file:")) None
    else {
      val idx = uri.indexOf('!')
      if (idx == -1) None
      else {
        val fileName = URLDecoder.decode(uri.substring("jar:file:".length(), idx), Charset.defaultCharset().name())
        val f = new File(fileName)
        if (f.exists) Some(f.getAbsolutePath())
        else None
      }
    }
  }

  private def findIDEBuildPluginEntry(name: String): String = {
    // if we're running in IDE, the unjarred classes are on the path, but there's a jarred
    // version of the plugin nearby...
    log.info(s"Unable to find $name on classpath. Trying to resolve from ide_build_intellij instead...")

    RuntimeScalaCompiler.classPathSeq.iterator
      .map(_.toString)
      .find(p => p.contains(name) && p.contains("classes"))
      .getOrElse(
        throw new RuntimeException(s"Unable to resolve $name from classpath: ${RuntimeScalaCompiler.classpath}"))
  }

  private val primitiveMapping = Map[Class[_], String](
    JavaBoolean.TYPE -> "Boolean",
    JavaCharacter.TYPE -> "Char",
    JavaByte.TYPE -> "Byte",
    JavaShort.TYPE -> "Short",
    JavaInteger.TYPE -> "Int",
    JavaLong.TYPE -> "Long",
    JavaFloat.TYPE -> "Float",
    JavaDouble.TYPE -> "Double",
    classOf[Object] -> "Any"
  ) // Any gets compiled down to Object in the class definitions, so we need to convert back to this wider type

  def isPrimitive(clazz: Class[_]) = primitiveMapping.contains(clazz)

  // Returns true if any right type visible constructor arguments mask left type val/defs.
  def rightMasksLeft(right: TypeInfo[_], left: TypeInfo[_]): (Boolean, Seq[String]) = {
    right.concreteClass
      .map(concreteClass => {

        // Get constructor args
        val constructorArgs = right.primaryConstructorParams.map(_._1)

        // Get map of method name -> parameter list
        val baseMethodArgListMap =
          concreteClass.getMethods.map(method => (method.getName, method.getParameterTypes.toList)).toMap

        // Visible constructor arguments have a zero arg method
        val visibleConstructorArguments = constructorArgs.filter(arg =>
          baseMethodArgListMap.get(arg).map {
            case Nil => true
            case _   => false
          } getOrElse (false))

        // Get map of method name -> parameter list for all extension classes.
        val extensionMethodArgListMap = left.classes
          .flatMap(extensionClass =>
            extensionClass.getMethods.map(method => (method.getName, method.getParameterTypes.toList)))
          .toMap ++ left.pureStructuralMethods.map(signature => (signature.getName, Nil)).toMap

        // Do any base type visible constructor arguments mask extension methods?
        val masked = visibleConstructorArguments.filter(arg =>
          extensionMethodArgListMap.get(arg).map {
            case Nil => true
            case _   => false
          } getOrElse (false))

        (!masked.isEmpty, masked)
      })
      .getOrElse((false, Nil))
  }
}
