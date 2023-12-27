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
package optimus.buildtool.compilers

import java.io.File
import java.io.OutputStream
import java.io.Reader
import java.io.StringReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections
import javax.tools.Diagnostic
import javax.tools.DiagnosticListener
import javax.tools.FileObject
import javax.tools.ForwardingJavaFileManager
import javax.tools.JavaFileManager
import javax.tools.JavaFileObject
import javax.tools.SimpleJavaFileObject
import javax.tools.StandardJavaFileManager
import javax.tools.ToolProvider
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.config.JavacConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import org.apache.commons.io.IOUtils

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

@entity trait AsyncJmhCompiler extends LanguageCompiler {
  @node final def classes(
      scopeId: ScopeId,
      inputs: NodeFunction0[AsyncJmhCompiler.Inputs]): Option[InternalClassFileArtifact] =
    output(scopeId, inputs).classes
  @node final def messages(scopeId: ScopeId, inputs: NodeFunction0[AsyncJmhCompiler.Inputs]): CompilerMessagesArtifact =
    output(scopeId, inputs).messages

  @node protected def output(scopeId: ScopeId, inputs: NodeFunction0[AsyncJmhCompiler.Inputs]): CompilerOutput
}
object AsyncJmhCompiler {
  final case class Inputs(
      localArtifacts: Seq[Artifact],
      otherArtifacts: Seq[Artifact],
      jmhJars: Seq[ClassFileArtifact],
      outputJar: JarAsset,
      javacConfig: JavacConfiguration
  )
}

@entity final class AsyncJmhCompilerImpl(sandboxFactory: SandboxFactory) extends AsyncJmhCompiler {
  @node def fingerprint: Seq[String] = Nil

  @node protected def output(scopeId: ScopeId, inputs0: NodeFunction0[AsyncJmhCompiler.Inputs]): CompilerOutput = {
    val inputs = inputs0()
    import inputs._

    def storeMessages(messages: List[CompilationMessage]): CompilerMessagesArtifact = {
      // Watched via `result.get.watchForDeletion()` below
      val messagesArtifact = CompilerMessagesArtifact.unwatched(
        id = InternalArtifactId(scopeId, ArtifactType.JmhMessages, None),
        messageFile = Utils.outputPathForType(outputJar, ArtifactType.JmhMessages).asJson,
        messages = messages,
        taskCategory = trace.Jmh,
        incremental = false
      )
      messagesArtifact.storeJson()
      messagesArtifact
    }

    def classArtifact: InternalClassFileArtifact =
      // Watched via `result.get.watchForDeletion()` below
      InternalClassFileArtifact.unwatched(
        InternalArtifactId(scopeId, ArtifactType.Jmh, None),
        outputJar,
        Hashing.hashFileOrDirectoryContent(outputJar),
        incremental = false
      )

    val jmhTrace = ObtTrace.startTask(scopeId, trace.Jmh)

    val result = Try {
      val (sources, resources, jmhMessages) = {
        log.info(s"[$scopeId:jmh] Starting generation")
        val gen = generateSources(inputs)
        log.info(s"[$scopeId:jmh] Completing generation")
        (gen.sources, gen.resources, gen.messages)
      }
      val erroneous = jmhMessages.exists(_.isError)
      if (erroneous) {
        CompilerOutput(None, storeMessages(jmhMessages), None)
      } else if (sources.isEmpty) { // for instance, if there are no benchmarks in this module despite a JMH dependency
        AssetUtils.atomicallyWrite(outputJar) { output =>
          // Return an empty JAR (~22 bytes) so that it looks like we succeeded
          JarUtils.jarFileSystem(JarAsset(output), create = true).close()
        }
        CompilerOutput(Some(classArtifact), storeMessages(jmhMessages), None)
      } else {
        val virtualSources = sources.map { case (path, content) =>
          VirtualizedJavaCompiler.InMemorySourceFile(SourceFileId(path, path), content)
        }
        val classpath = (localArtifacts ++ otherArtifacts).collect { case classes: ClassFileArtifact =>
          classes.file.pathString
        }

        val messages = AssetUtils.atomicallyWrite(outputJar) { output =>
          val outfs = JarUtils.jarFileSystem(JarAsset(output), create = true)
          val javacMessages =
            try {
              log.info(s"[$scopeId:jmh] Starting compilation")
              resources.toList.sortBy(_._1).foreach { // sort resources for stability
                case (relpath, content) =>
                  val outpath = outfs getPath relpath.toString
                  Files.createDirectories(outpath.getParent)
                  Files.write(outpath, content)
              }
              VirtualizedJavaCompiler.compile(
                outfs,
                classpath,
                virtualSources.toList,
                javacConfig = javacConfig.resolvedOptions)
            } catch {
              case e: CompilationException => throw e
              case NonFatal(e) =>
                throw new CompilationException(
                  scopeId,
                  "JMH compilation failed",
                  Nil,
                  e,
                  ArtifactType.JmhMessages,
                  trace.Jmh)
            } finally {
              outfs.close()
              log.info(s"[$scopeId:jmh] Completing compilation")
            }
          storeMessages(jmhMessages ++ javacMessages)
        }

        CompilerOutput(Some(classArtifact), messages, None)
      }
    }

    jmhTrace.complete(result.map(_.messages.messages))
    result.get.watchForDeletion()
  }

  import AsyncJmhCompilerImpl._

  private def generateSources(inputs: AsyncJmhCompiler.Inputs): SourceGenerationResult = {
    import CompilationMessage._

    val jmhGenClasspath = (inputs.jmhJars ++ inputs.localArtifacts ++ inputs.otherArtifacts).collect {
      case classes: ClassFileArtifact => classes.pathString
    }.toArray
    val inputClasses = inputs.localArtifacts.collect { case classes: ClassFileArtifact =>
      classes
    }

    // There's a nice ASM-based generator that reads the class files and directly emits strings and all.
    // However we can't use it because it only supports Java 1.4.
    // We used to have a monkey-patch in OBT, but that
    //   1) restricts redistribution, since that's GPL code, and
    //   2) stops us from upgrading, since it's got to be statically linked here.
    // #2 can be remediated by a bit of runtime fun with classloaders (an absolute joy, actually!),
    // but a more easily-maintained solution is preferred, which this (purportedly) is.
    // The various uses of *unsafe directory methods can be justified as the temporary directories do not escape this method.
    val sandbox = sandboxFactory.empty("jmh")
    try {
      val inClasses = sandbox.sourceDir
      inputClasses.foreach { jar =>
        Jars.withJar(jar.file, create = false) { root =>
          Utils.recursivelyCopy(root, inClasses, filter = Directory.fileExtensionPredicate("class"))
        }
      }
      val outSources = sandbox.outputDir("output-sources")
      val outResources = sandbox.outputDir("output-resources")

      val javaExec = s"${sys.props("java.home")}${File.separator}bin${File.separator}java"
      val jmhClasspath = jmhGenClasspath.mkString(File.pathSeparator)
      val jmhMain = "org.openjdk.jmh.generators.bytecode.JmhBytecodeGenerator"
      val jmhArgs = List(inClasses.pathString, outSources.pathString, outResources.pathString, "reflection")
      // Write arguments to a file to prevent the command line from being too long on Windows
      val argFile = sandbox.outputFile("args.txt")
      Files.write(argFile.path, ("-cp" :: jmhClasspath :: jmhMain :: jmhArgs).asJava)
      val outFile = sandbox.outputFile("log.out")
      val errFile = sandbox.outputFile("log.err")
      val proc = new ProcessBuilder(javaExec, s"@${argFile.path.toString}")
        .directory(sandbox.root.path.toFile)
        .redirectOutput(outFile.path.toFile)
        .redirectError(errFile.path.toFile)
        .start()

      def messagesFrom(file: FileAsset, level: Severity): Option[CompilationMessage] = {
        import scala.io._
        file.existsUnsafe opt {
          val src = Source.fromFile(file.path.toFile)
          try {
            val content = src.mkString.trim
            content.nonEmpty opt {
              message(content, level)
            }
          } finally src.close()
        } flatten
      }

      val messages = List(messagesFrom(outFile, Severity.Info), messagesFrom(errFile, Severity.Error)).flatten

      proc.waitFor() match {
        case 0 =>
          val sourceFiles = Directory.findFilesUnsafe(outSources)
          val resourceFiles = Directory.findFilesUnsafe(outResources)
          val sources = sourceFiles map { src =>
            outSources.relativize(src) -> new String(Files.readAllBytes(src.path))
          }
          val resources = resourceFiles map { rsrc =>
            outResources.relativize(rsrc) -> sortLines(Files.readAllBytes(rsrc.path))
          }
          SourceGenerationResult(sources.toMap, resources.toMap, messages)
        case code =>
          val errorText = IOUtils.toString(errFile.path.toUri, "UTF-8")
          val errors = message(s"JMH generator exited with return code $code", Error) ::
            errorText.split("\n").filterNot(_.trim.isEmpty).map(message(_, Error)).toList
          log.error(s"out: ${IOUtils.toString(outFile.path.toUri, "UTF-8")}")
          log.error(s"err: $errorText")
          SourceGenerationResult(Map.empty, Map.empty, errors ++ messages)

      }
    } finally sandbox.close()
  }
}

object AsyncJmhCompilerImpl {
  output.setCustomCache(NodeCaching.reallyBigCache)

  private final case class SourceGenerationResult(
      sources: Map[RelativePath, String],
      resources: Map[RelativePath, Array[Byte]],
      messages: List[CompilationMessage])

  // JMH doesn't reliably produce sorted resources; this keeps compilation RT
  private def sortLines(ba: Array[Byte]): Array[Byte] = {
    val decoded = new String(ba, StandardCharsets.UTF_8) // jmh always uses UTF-8, thank goodness
    val sortedLines = decoded.linesIterator.toList.sorted
    sortedLines.mkString("\n").getBytes(StandardCharsets.UTF_8)
  }
}

/**
 * A wrapper around the Java compiler which supports reading from virtual (in memory) source files and writing directly
 * to .jar files using NIO virtual filesystem.
 */
@entity private[buildtool] object VirtualizedJavaCompiler {
  final case class Inputs(
      sourceFiles: Map[SourceUnitId, HashedContent],
      outputPath: JarAsset,
      inputArtifacts: Seq[Artifact],
      javacConfig: Seq[String]
  )

  @node def fingerprint: Seq[String] = Seq(s"[Java]${Utils.javaClassVersionTag}")

  @node protected def compilerOutput(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): CompilerOutput = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val msgPath = Utils.outputPathForType(outputPath, ArtifactType.JavaMessages).asJson

    if (sourceFiles.nonEmpty) {
      val taskTrace = ObtTrace.startTask(scopeId, Java)
      val result = Try {
        val classpath = inputArtifacts.collect { case c: ClassFileArtifact => c.file.pathString }

        val sourcesInMemory = sourceFiles.map { case (id, content) =>
          InMemorySourceFile(id, content.utf8ContentAsString)
        }

        val msgArtifacts = AssetUtils.atomicallyWrite(outputPath) { tempOut =>
          val tempFs = JarUtils.jarFileSystem(JarAsset(tempOut), create = true)
          log.info(s"[$scopeId] Starting java compilation")
          log.debug(
            s"[$scopeId] Java paths: $tempOut -> ${outputPath.path}, args=${javacConfig.mkString("[", " ", "]")}")
          val messages =
            try {
              compile(tempFs, classpath, sourcesInMemory.toIndexedSeq, javacConfig)
            } catch {
              case e: CompilationException => throw e
              case NonFatal(e) =>
                throw new CompilationException(
                  scopeId,
                  "Java compilation failed",
                  Nil,
                  e,
                  ArtifactType.JavaMessages,
                  Java
                )
            } finally tempFs.close()

          val cancelScope = EvaluationContext.cancelScope
          // important not to move the file if we were cancelled during compilation because it might be incomplete
          if (cancelScope.isCancelled) throw cancelScope.cancellationCause
          else {
            // Watched via `result.get.watchForDeletion()` below
            val msgArtifacts = CompilerMessagesArtifact.unwatched(
              InternalArtifactId(scopeId, ArtifactType.JavaMessages, None),
              msgPath,
              messages,
              Java,
              incremental = false
            )
            msgArtifacts.storeJson()

            // we don't incrementally rewrite these jars at the moment, so might as well compress to save disk space
            if (!msgArtifacts.hasErrors)
              Jars.stampJarWithConsistentHash(JarAsset(tempOut), compress = true)

            msgArtifacts
          }
        }

        if (msgArtifacts.hasErrors) {
          val output = CompilerOutput(None, msgArtifacts, None)
          Utils.FailureLog.error(s"[$scopeId] Completing java compilation with errors")
          log.debug(s"[$scopeId] Java errors: $output")
          output
        } else {
          val output = CompilerOutput(
            Some(
              // Watched via `result.get.watchForDeletion()` below
              InternalClassFileArtifact.unwatched(
                InternalArtifactId(scopeId, ArtifactType.Java, None),
                outputPath,
                Hashing.hashFileOrDirectoryContent(outputPath),
                incremental = false
              )
            ),
            msgArtifacts,
            None
          )
          log.info(s"[$scopeId] Completing java compilation")
          log.debug(s"[$scopeId] Java output: $output")
          output
        }
      }
      taskTrace.complete(result.map(_.messages.messages))
      result.get.watchForDeletion()
    } else CompilerOutput.empty(scopeId, ArtifactType.JmhMessages, msgPath)
  }

  private[compilers] def compile(
      fs: FileSystem,
      cp: Seq[String],
      sources: Seq[InMemorySourceFile],
      javacConfig: Seq[String]
  ): Seq[CompilationMessage] = {
    val tempRoot = fs.getRootDirectories.iterator().next()

    val opts = (List("-cp", cp.mkString(File.pathSeparator), "-g") ++ javacConfig).asJava

    var fileManager: StandardJavaFileManager = null
    var outputFileManager: NioWriteOnlyJavaFileManager = null
    try {
      fileManager = ToolProvider.getSystemJavaCompiler.getStandardFileManager(null, null, null)
      outputFileManager = new NioWriteOnlyJavaFileManager(tempRoot, fileManager)
      val messages = mutable.ArrayBuffer[CompilationMessage]()
      val diagnosticListener = new DiagnosticListener[JavaFileObject] {
        override def report(diagnostic: Diagnostic[_ <: JavaFileObject]): Unit = {
          if (diagnostic ne null) {
            import javax.tools.Diagnostic.Kind._
            val severity = diagnostic.getKind match {
              case ERROR                       => CompilationMessage.Error
              case WARNING | MANDATORY_WARNING => CompilationMessage.Warning
              case NOTE | OTHER                => CompilationMessage.Info
            }

            messages += CompilationMessage(
              Option(diagnostic.getSource).map { _ =>
                MessagePosition(
                  filepath = diagnostic.getSource.getName.stripPrefix("/"),
                  startLine = diagnostic.getLineNumber.toInt,
                  endLine = diagnostic.getLineNumber.toInt,
                  startColumn = diagnostic.getColumnNumber.toInt,
                  endColumn = diagnostic.getColumnNumber.toInt,
                  startPoint = diagnostic.getStartPosition.toInt,
                  endPoint = diagnostic.getEndPosition.toInt
                )
              },
              msg = diagnostic.getMessage(null),
              severity = severity
            )
          }
        }
      }

      val compileTask =
        ToolProvider.getSystemJavaCompiler.getTask(
          null,
          outputFileManager,
          diagnosticListener,
          opts,
          null,
          sources.asJava)
      compileTask.setProcessors(Collections.emptyList())
      compileTask.call()
      messages.toIndexedSeq
    } finally {
      if (outputFileManager ne null)
        outputFileManager.close()
      if (fileManager ne null)
        fileManager.close()
    }
  }

  /**
   * Supports writing classfiles under the supplied NIO Path (which could for example be inside a .jar file or an
   * in-memory file system). Doesn't support any other JavaFileManager operations (but then again no other operations
   * seem to ever get called).
   */
  class NioWriteOnlyJavaFileManager(outPath: Path, delegate: JavaFileManager)
      extends ForwardingJavaFileManager[JavaFileManager](delegate) {
    override def getJavaFileForOutput(
        location: JavaFileManager.Location,
        className: String,
        kind: JavaFileObject.Kind,
        sibling: FileObject): JavaFileObject = {
      val relativeFilePath = className.replace('.', '/') + kind.extension
      val filePath = outPath.resolve(relativeFilePath)
      // entirely fake URI - doesn't seem to be used for anything but we have to supply it
      val fakeURI = new URI(s"niojavafilemanager://$relativeFilePath")
      new SimpleJavaFileObject(fakeURI, kind) {
        override def openOutputStream(): OutputStream = {
          Files.createDirectories(filePath.getParent)
          Files.newOutputStream(filePath)
        }
      }
    }

    override def isSameFile(a: FileObject, b: FileObject): Boolean = a == b
  }

  /**
   * A SimpleJavaFileObject representing a source file as a String of content
   */
  final case class InMemorySourceFile(id: SourceUnitId, content: String)
      extends SimpleJavaFileObject(
        new URI("obtJavaPath", null, "/" + id.localRootToFilePath.pathString, null, null),
        JavaFileObject.Kind.SOURCE) {
    override def openReader(ignoreEncodingErrors: Boolean): Reader = new StringReader(content)
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = content
  }

  // This is the node through which java compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  compilerOutput_info.setCustomCache(reallyBigCache)
}
