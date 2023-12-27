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
package optimus.buildtool.generators

import com.sun.codemodel.JPackage
import com.sun.codemodel.writer.FileCodeWriter
import com.sun.tools.xjc.Plugin

import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import com.sun.tools.xjc.api.ErrorListener
import com.sun.tools.xjc.api.SchemaCompiler
import com.sun.tools.xjc.api.SpecVersion
import com.sun.tools.xjc.api.XJC
import com.sun.tools.xjc.api.impl.s2j.SchemaCompilerImpl
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GeneratedSourceArtifact
import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.EndsWithFilter
import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.JaxbGenerator.JaxbType
import optimus.buildtool.generators.JaxbGenerator.ObtCodeWriter
import optimus.buildtool.generators.JaxbGenerator.ObtEntityResolver
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log
import org.apache.cxf.tools.common.ToolContext
import org.apache.cxf.tools.common.ToolErrorListener
import org.apache.cxf.tools.wsdlto.WSDLToJava
import org.xml.sax.EntityResolver
import org.xml.sax.SAXParseException

import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStreamWriter
import java.io.Writer
import java.net.URL
import java.net.URLConnection
import java.net.URLStreamHandler
import java.net.spi.URLStreamHandlerProvider
import scala.annotation.nowarn
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.xml.InputSource

@entity class JaxbGenerator(directoryFactory: DirectoryFactory, workspaceSourceRoot: Directory)
    extends SourceGenerator {
  override val artifactType: GeneratedSourceArtifactType = ArtifactType.Jaxb

  override type Inputs = JaxbGenerator.Inputs

  @node override protected def _inputs(
      name: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourceFilter: PathFilter,
      configuration: Map[String, String],
      scope: CompilationScope
  ): Inputs = {

    val jaxbType = JaxbType.parse(configuration.get("jaxbType"))
    val filter = sourceFilter && JaxbGenerator.sourcePredicate(jaxbType)

    val (templateFiles, templateFingerprint) = SourceGenerator.rootedTemplates(
      internalFolders,
      externalFolders,
      filter,
      scope,
      workspaceSourceRoot,
      s"Template:$name"
    )

    val pkg = configuration.get("package")
    val pkgs = configuration.get("packages").map(_.split(",").toIndexedSeq).getOrElse(Nil)
    val plugins: Seq[Plugin] =
      configuration.collect {
        case (k, v) if k.startsWith("plugins.") => Class.forName(v).getConstructor().newInstance().asInstanceOf[Plugin]
      }.toIndexedSeq

    val (bindingFiles, bindingFingerprint) =
      configuration
        .get("bindings")
        .map { b =>
          val paths = b.split(",").map(RelativePath(_)).toIndexedSeq
          SourceGenerator.templates(
            internalFolders,
            externalFolders,
            EndsWithFilter(paths: _*),
            scope,
            workspaceSourceRoot,
            s"Binding:$name"
          )
        }
        .unzipOption

    // Allow other files (which may be included by the templateFiles) to be part of the fingerprint
    val (rootFiles, rootFingerprint) = configuration
      .get("root")
      .map { s =>
        val p = Paths.get(s)
        val (internalRoots, externalRoots) =
          if (!p.isAbsolute) {
            val d =
              directoryFactory
                .lookupSourceFolder(workspaceSourceRoot, scope.config.paths.absScopeRoot.resolveDir(RelativePath(p)))
            (Seq(d), Nil)
          } else {
            val d = directoryFactory.lookupDirectory(p)
            (Nil, Seq(d))
          }
        SourceGenerator.rootedTemplates(
          internalRoots,
          externalRoots,
          NoFilter, // deliberately use NoFilter here, to allow for non-standard filenames
          scope,
          workspaceSourceRoot,
          s"Include:$name"
        )
      }
      .unzipOption

    val fingerprint =
      s"[JAXB]${SourceGenerator.location[XJC].pathFingerprint}" +: (
        pkg.map(p => s"[Package]$p").toIndexedSeq ++
          pkgs.map(p => s"[Packages]$p") ++
          plugins.map(p => s"[Plugins]${p.getOptionName}") ++
          templateFingerprint ++
          bindingFingerprint.getOrElse(Nil) ++
          rootFingerprint.getOrElse(Nil)
      )
    val fingerprintHash = scope.hasher.hashFingerprint(fingerprint, ArtifactType.GenerationFingerprint)

    JaxbGenerator.Inputs(
      name,
      jaxbType,
      pkg,
      pkgs,
      templateFiles,
      bindingFiles.getOrElse(SortedMap.empty),
      rootFiles.map(_.map(_._2).merge).getOrElse(SortedMap.empty),
      fingerprintHash,
      plugins
    )
  }

  @node override def containsRelevantSources(inputs: NodeFunction0[Inputs]): Boolean =
    inputs().templateFiles.map(_._2).merge.nonEmpty

  @node override def generateSource(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      outputJar: JarAsset
  ): Option[GeneratedSourceArtifact] = JaxbGenerator.oneAtTheTime(asNode { () =>
    val resolvedInputs = inputs()
    import resolvedInputs._
    val allTemplates = templateFiles.map(_._2).merge
    ObtTrace.traceTask(scopeId, GenerateSource) {
      val artifact = Utils.atomicallyWrite(outputJar) { tempOut =>
        val tempJar = JarAsset(tempOut)
        // Use a short temp dir name to avoid issues with too-long paths for generated .java files
        val tempDir = Directory(Files.createTempDirectory(tempJar.parent.path, ""))
        val outputDir = tempDir.resolveDir(JaxbGenerator.SourcePath)
        Utils.createDirectories(outputDir)

        val msgs = jaxbType match {
          case JaxbType.Xsd =>
            val compiler = createSchemaCompiler()
            val listener = new JaxbGenerator.ObtXjcErrorListener(scopeId, generatorName)
            compiler.setErrorListener(listener)
            pkg.foreach(compiler.setDefaultPackageName)
            compiler.setTargetVersion(SpecVersion.V2_1)

            // noinspection ScalaDeprecation
            val compilerSettings =
              compiler.getOptions: @nowarn("msg=method getOptions in trait SchemaCompiler is deprecated")

            // prepare jaxb2 plugins here
            if (plugins.nonEmpty) {
              compilerSettings.getAllPlugins.addAll(plugins.asJava)
              plugins.foreach(plugin => compilerSettings.parseArgument(Array(s"-${plugin.getOptionName}"), 0))
            }

            // noinspection ScalaDeprecation
            bindingFiles.foreach { case (f, c) =>
              val s = new InputSource(c.contentAsInputStream)
              s.setSystemId(JaxbGenerator.systemId(workspaceSourceRoot, f))
              compilerSettings.addBindFile(s)
            }

            compiler.setEntityResolver(new ObtEntityResolver(workspaceSourceRoot, rootFiles))

            allTemplates.foreach { case (f, c) =>
              val s = new InputSource(c.contentAsInputStream)
              s.setSystemId(JaxbGenerator.systemId(workspaceSourceRoot, f))
              compiler.parseSchema(s)
            }

            val model = compiler.bind()

            val success = if (model != null) {
              val cm = model.generateCode(null, listener)
              val writer = new ObtCodeWriter(outputDir.path.toFile, "UTF-8")
              cm.build(writer)
              true
            } else {
              log.debug(s"[$scopeId:$generatorName] JAXB generation failed")
              false
            }

            if (listener.messages.nonEmpty || success) listener.messages
            else Seq(CompilationMessage(None, "JAXB generation failed", CompilationMessage.Error))

          case JaxbType.Wsdl =>
            val validatedTemplates = templateFiles.apar.map { case (d, files) =>
              SourceGenerator.validateFiles(d, files)
            }
            val validatedTemplateFiles = validatedTemplates.flatMap(_.files)
            validatedTemplateFiles
              .flatMap { f =>
                val args = Seq("-autoNameResolution") ++
                  pkgs.flatMap(p => Seq("-p", s"$p")) ++
                  Seq("-d", outputDir.pathString, f.pathString)

                val wsdlToJava = new WSDLToJava(args.toArray)
                val c = new ToolContext
                val listener = new JaxbGenerator.ObtWsdlErrorListener(scopeId, generatorName)
                c.setErrorListener(listener)
                wsdlToJava.run(c)
                listener.messages
              }
        }
        val a = GeneratedSourceArtifact.create(
          scopeId,
          generatorName,
          artifactType,
          outputJar,
          JaxbGenerator.SourcePath,
          msgs
        )
        SourceGenerator.createJar(generatorName, JaxbGenerator.SourcePath, a.messages, a.hasErrors, tempJar, tempDir)()
        a
      }
      Some(artifact)
    }
  })

  protected def createSchemaCompiler(): SchemaCompiler = new SchemaCompilerImpl()
}

object JaxbGenerator extends Log {

  // xjc is not thread-safe, so we need to make sure we don't call it from more than one thread at the same time
  // see https://javaee.github.io/jaxb-v2/doc/user-guide/ch06.html#d0e6879 and
  // https://discuss.gradle.org/t/parallelizable-jaxb-xjc-plugin-task/14855
  private val oneAtTheTime = new AdvancedUtils.Throttle(1)

  private val SourcePath = RelativePath("src")
  private def sourcePredicate(t: JaxbType) = t match {
    case JaxbType.Xsd  => Directory.fileExtensionPredicate("xsd")
    case JaxbType.Wsdl => Directory.fileExtensionPredicate("wsdl")
  }

  def systemId(workspaceSourceRoot: Directory, f: FileAsset): String = {
    val urlPath = s"//${workspaceSourceRoot.relativize(f).pathString}"
    // use an "obt" scheme to prevent xjc thinking it knows best about where the URI is located. we also
    // strip out workspace location to minimize location dependence in generated sources.
    new URI(ObtProtocol.Scheme, null, urlPath, null).toString
  }

  final case class Inputs(
      generatorName: String,
      jaxbType: JaxbType,
      pkg: Option[String],
      pkgs: Seq[String],
      templateFiles: Seq[(Directory, SortedMap[FileAsset, HashedContent])],
      bindingFiles: SortedMap[FileAsset, HashedContent],
      rootFiles: SortedMap[FileAsset, HashedContent],
      fingerprintHash: String,
      plugins: Seq[Plugin]
  ) extends SourceGenerator.Inputs

  sealed trait JaxbType
  object JaxbType {
    case object Xsd extends JaxbType
    case object Wsdl extends JaxbType

    def parse(typeStr: Option[String]): JaxbType = typeStr match {
      case Some("wsdl") => Wsdl
      case _            => Xsd // default to Xsd
    }
  }

  class ObtCodeWriter(target: File, encoding: String) extends FileCodeWriter(target, false, encoding) {
    // Override to skip the UnicodeEscapeWriter wrapping, since that gives inconsistent results on different platforms
    // (due to the fact that EncoderFactory.createEncoder looks at a system property rather than the argument it
    // is passed).
    override def openSource(pkg: JPackage, fileName: String): Writer =
      new OutputStreamWriter(openBinary(pkg, fileName), encoding)
  }

  class ObtEntityResolver(
      workspaceSourceRoot: Directory,
      rootFiles: SortedMap[FileAsset, HashedContent]
  ) extends EntityResolver {
    private val inputs: SortedMap[String, InputSource] = rootFiles.map { case (f, hc) =>
      val systemId = JaxbGenerator.systemId(workspaceSourceRoot, f)
      val source = new InputSource(hc.contentAsInputStream)
      source.setSystemId(systemId)
      systemId -> source
    }

    override def resolveEntity(publicId: String, systemId: String): InputSource =
      inputs.getOrElse(systemId, null)
  }

  class ObtXjcErrorListener(id: ScopeId, generatorName: String) extends ErrorListener {
    private[generators] val _messages = mutable.Buffer[CompilationMessage]()

    private def append(severity: CompilationMessage.Severity, e: SAXParseException): Unit = {
      log.debug(s"[$id:$generatorName] $severity: $e", e)
      _messages += CompilationMessage(None, e.toString, severity)
    }

    override def error(e: SAXParseException): Unit = append(CompilationMessage.Error, e)
    override def fatalError(e: SAXParseException): Unit = append(CompilationMessage.Error, e)
    override def warning(e: SAXParseException): Unit = append(CompilationMessage.Warning, e)
    override def info(e: SAXParseException): Unit = append(CompilationMessage.Info, e)

    def messages: Seq[CompilationMessage] = _messages.toIndexedSeq
  }

  class ObtWsdlErrorListener(id: ScopeId, generatorName: String) extends ToolErrorListener {
    private val _messages = mutable.Buffer[CompilationMessage]()

    override def addError(file: String, line: Int, column: Int, message: String): Unit = {
      _messages += CompilationMessage(position(file, line, column), message, CompilationMessage.Error)
      super.addError(file, line, column, message)
    }
    override def addError(file: String, line: Int, column: Int, message: String, t: Throwable): Unit = {
      _messages += CompilationMessage(position(file, line, column), message, CompilationMessage.Error)
      log.debug(s"[$id:$generatorName] $message", t)
      super.addError(file, line, column, message, t)
    }
    override def addWarning(file: String, line: Int, column: Int, message: String): Unit = {
      _messages += CompilationMessage(position(file, line, column), message, CompilationMessage.Error)
      // don't call super, since all it does is log
    }
    override def addWarning(file: String, line: Int, column: Int, message: String, t: Throwable): Unit = {
      _messages += CompilationMessage(position(file, line, column), message, CompilationMessage.Error)
      log.debug(s"[$id:$generatorName] $message", t)
      // don't call super, since all it does is log
    }

    def messages: Seq[CompilationMessage] = _messages.toIndexedSeq

    private def position(file: String, line: Int, column: Int): Option[MessagePosition] =
      Some(MessagePosition(file, line, column, line, column, -1, -1))
  }
}

object ObtProtocol {
  val Scheme = "obt"
}
//noinspection ScalaUnusedSymbol - loaded by virtue of inclusion in META-INF/services/java.net.spi.URLStreamHandlerProvider
// this exists just to keep happy when creating URLs that start "obt://"
final class ObtProtocol extends URLStreamHandlerProvider {
  private object Handler extends URLStreamHandler {
    // Note: We need to provide an implementation here, but the content can be empty
    override def openConnection(u: URL): URLConnection = new EmptyConnection(u)
  }
  private class EmptyConnection(url: URL) extends URLConnection(url) {
    override def connect(): Unit = ()
    override def getInputStream: InputStream = new ByteArrayInputStream(Array.emptyByteArray)
  }
  override def createURLStreamHandler(protocol: String): URLStreamHandler =
    if (protocol == ObtProtocol.Scheme) Handler else null
}
