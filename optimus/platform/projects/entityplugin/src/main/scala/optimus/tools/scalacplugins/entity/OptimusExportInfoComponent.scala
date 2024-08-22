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
package optimus.tools.scalacplugins.entity

import java.io.File
import java.nio.file.FileSystems
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import optimus.config.spray.json.JsonParser
import optimus.platform.metadatas.internal._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.reflect.io.{VirtualDirectory => ReflectVirtualDirectory}
import scala.tools.nsc.Phase
import scala.tools.nsc.io.{VirtualDirectory => NscVirtualDirectory}
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.CollectionUtils.SingleCollection

import scala.annotation.nowarn

/**
 * Writes metadata about entities, embeddables and events to files (entity.json, stored.json, embeddable.json and
 * event.json) in the output directory. This metadata is used at runtime by the EntityHierarchyManager.
 *
 * As soon as the phase is created, background workers load the existing metadata files and scan names of existing class
 * files (if any - this is needed in the case of incremental builds). Later on when the phase is run, we scan the AST to
 * generate metadata for newly compiled types, combine this with existing metadata and (re)write the metadata files.
 */
class OptimusExportInfoComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with PluginUtils
    with TypedUtils
    with WithOptimusPhase {
  import global._

  val diagnostics = false

  sealed trait EntityTypes
  case object IsEmbeddable extends EntityTypes
  case object HasSensitiveField extends EntityTypes

  private def currentOutputDir: Option[Path] = {
    if (plugin.settings.disableExportInfo)
      None
    else {
      // this would looks like "workspace/build/dal_client/classes"
      // we presume there is only one output directory per compilation run
      val outputDirs = global.settings.outputDirs
      val dir = outputDirs.getSingleOutput.getOrElse {
        val outputs = outputDirs.outputs.map { case (src, dest) => dest }.distinct
        if (outputs.size == 1) outputs.head
        else throw new IllegalArgumentException(s"We only support one output dir, but found: $outputs")
      }
      // don't generate metadata for REPL compilation or tests
      if (dir.isInstanceOf[NscVirtualDirectory] || dir.isInstanceOf[ReflectVirtualDirectory])
        None
      else
        Some(Paths.get(dir.toURL.toURI))
    }
  }

  override def newPhase(prev: Phase): Phase = {
    currentOutputDir
      .map { dir =>
        // start the concurrent worker Futures now so that they are hopefully done by the time we need the results
        val workers = new MetadataConcurrentWorkers(dir, settings.classpath.value)

        new OptimusExportInfo(prev, workers)
      }
      .getOrElse {
        new OptimusNoOpPhase(prev)
      }
  }

  class OptimusNoOpPhase(prev: Phase) extends EntityPluginStdPhase(prev) {
    override def run(): Unit = {}
    override def apply0(unit: global.CompilationUnit): Unit = {}
  }

  object MetaType extends Enumeration {
    type MetaType = Value
    val PackageMeta: MetaType = Value("Package")
    val Meta: MetaType = Value("Meta")
  }

  // The content of generated json files are guaranteed sorted by full class name
  class OptimusExportInfo(prev: Phase, workers: MetadataConcurrentWorkers) extends EntityPluginStdPhase(prev) {
    import MetaDataFiles._
    import MetaType._

    val projOutputDir: Path = workers.outputDir

    override def name: String = phaseInfo.phaseName

    object MetaFieldPackage {
      def unapply(tree: Tree): Option[Tree] = tree match {
        case id @ Ident(_) if id.symbol.hasPackageFlag => Some(id)
        case Select(MetaFieldPackage(_), _)            => Some(tree)
        case _                                         => None
      }
    }

    case class CatalogingMetadata(owner: String, catalog: String)

    case class Metadatas(
        entities: Map[String, EntityBaseMetaData],
        storables: Map[String, EntityBaseMetaData],
        embeddables: Map[String, EmbeddableBaseMetaData],
        events: Map[String, EventBaseMetaData],
        metas: Map[String, MetaBaseMetaData])

    // build metadata map(fullClassName -> MetaData) for entites, storables, embeddables and events respectively
    private def buildMetadataMap(classes: List[MemberDef]): Metadatas = {
      // mutable data used for performance to build result
      // the result is immutable
      // there is no multi-threaded access in this method
      // and the mutable data does not escape the method
      val entities = mutable.Map.empty[String, EntityBaseMetaData]
      val storables = mutable.Map.empty[String, EntityBaseMetaData]
      val embeddables = mutable.Map.empty[String, EmbeddableBaseMetaData]
      val events = mutable.Map.empty[String, EventBaseMetaData]
      val metas = mutable.Map.empty[String, MetaBaseMetaData]

      // map maintaining Type and Boolean(True/False) based on if type extends from Trait PIIElement
      val sensitiveTypeCache = mutable.Map.empty[Type, Boolean]

      // deliberately not using foldLeft here for reduce datastructure copy
      classes.foreach { clz =>
        val classSym = clz.symbol
        val fullClassName = if (classSym.isModuleClass) classSym.fullName + "$" else classSym.fullName
        val packageName = classSym.enclosingPackage.fullName
        val isMeta = classSym.hasAnnotation(MetaAnnotation)
        val isStorable = classSym.hasAnnotation(StoredAnnotation)
        val isObject = classSym.isModuleClass
        val isTrait = classSym.isTrait
        val parentNames = classSym.parentSymbols.map(_.fullName)
        lazy val inheritedMetadata: Option[CatalogingMetadata] = {
          classSym.parentSymbols.find(parent =>
            parent.hasAnnotation(MetaAnnotation) && parent.hasAnnotation(StoredAnnotation)) match {
            case Some(parent) => Some(extractMetaParams(parent, Meta))
            case None         => None
          }
        }

        def exportCataloguingMetadata(metaCatalog: CatalogingMetadata, isInherited: Boolean = false): Unit = {
          // we are manually adding the catalogs to each entities tagged
          // either by package object, inheritance, directly tagged or Opted out
          val className =
            if (isInherited) fullClassName.concat(OptimusExportInfoComponent.InheritedMetaAnnotationSuffix)
            else fullClassName
          val metaSquared = MetaBaseMetaData(
            className,
            packageName,
            metaCatalog.owner,
            metaCatalog.catalog,
            classSym.hasAnnotation(EntityAnnotation),
            classSym.hasAnnotation(StoredAnnotation),
            isObject,
            isTrait,
            parentNames
          )
          metas += (fullClassName -> metaSquared)
        }

        def isFieldTypeSensitive(tpe: Type): Boolean = {
          // Cached :: to not repeat the expensive search over all different Types
          sensitiveTypeCache.getOrElseUpdate(tpe, tpe.baseClasses.contains(piiElement))
        }

        def isDataSubjectCategory(sym: Symbol): Boolean = {
          !sym.isTrait && !sym.isRefinementClass && sym.baseClasses.contains(dataSubjectCategory.companion)
        }

        def getMemberType(member: Symbol): Option[EntityTypes] = {
          if (member.isField && isEmbeddable(member.tpe.typeSymbol)) Some(IsEmbeddable)
          else if (member.isField && isFieldTypeSensitive(member.typeOfThis)) Some(HasSensitiveField)
          else None
        }

        def getPIIDetailsForSymbol(sym: Symbol, prefix: Option[String]): PIIDetails = {
          PIIDetails(
            name = s"${prefix.getOrElse("")}${sym.nameString.replace(suffixes.IMPL, "")}",
            dataSubjectCategories = sym.originalInfo.typeArguments.single.baseClasses
              .filter(baseClass => isDataSubjectCategory(baseClass))
              .map(_.nameString.split('$').last),
            fullyQualifiedName = sym.typeOfThis.directObjectString
          )
        }

        def handleEntityMetaData(sym: Symbol): Unit = {
          val isAbstract = sym.isAbstract
          val anno = sym.getAnnotation(EntityMetaDataAnnotation).get
          val annoParams = anno.assocs.toMap
          val slotNumber = annoParams.get(entityMetaDataAnnotationNames.slotNumber) match {
            case Some(LiteralAnnotArg(l)) => l.value.asInstanceOf[Int]
            case _                        => 0
          }
          val explicitSlotNumber = annoParams.get(entityMetaDataAnnotationNames.explicitSlotNumber) match {
            case Some(LiteralAnnotArg(l)) => l.value.asInstanceOf[Boolean]
            case _                        => false
          }

          // Note:: Trait having sensitiveField will still have empty piiElements List as traits don't have fields but methods
          def getPIIElementList(sym: Symbol, visited: Set[Type], prefix: Option[String]): Seq[PIIDetails] = {
            sym.info.members.foldLeft(List.empty[PIIDetails]) { (list, member) =>
              getMemberType(member) match {
                case Some(HasSensitiveField) => list :+ getPIIDetailsForSymbol(member, prefix)
                case Some(IsEmbeddable) if (!visited.contains(member.typeSignature)) =>
                  val newVisited = visited + member.typeSignature
                  val embeddablePrefix = s"${prefix.getOrElse("")}${member.nameString}."
                  list ++ getPIIElementList(member.thisSym, newVisited, Option(embeddablePrefix))
                case _ => list
              }
            }
          }

          val piiElementList = getPIIElementList(sym, Set.empty, None)

          val entityMetaData = EntityBaseMetaData(
            fullClassName,
            packageName,
            isStorable,
            isAbstract,
            isObject,
            isTrait,
            slotNumber,
            explicitSlotNumber,
            parentNames,
            piiElementList)

          if (isStorable) {
            val storableParentNames = classSym.parentSymbols.filter(_.hasAnnotation(StoredAnnotation)).map(_.fullName)
            val storableMetaData = EntityBaseMetaData(
              fullClassName,
              packageName,
              isStorable,
              isAbstract,
              isObject,
              isTrait,
              slotNumber,
              explicitSlotNumber,
              storableParentNames,
              piiElementList)
            entities += (fullClassName -> entityMetaData)
            storables += (fullClassName -> storableMetaData)
          } else {
            entities += (fullClassName -> entityMetaData)
          }
        }

        if (classSym.hasAnnotation(EntityAnnotation)) {
          // if metadata is available only through inheritance, collect it
          if (isStorable && inheritedMetadata.isDefined && !isMeta) {
            exportCataloguingMetadata(inheritedMetadata.get, true)
          }
          handleEntityMetaData(classSym)
        } else if (classSym.hasAnnotation(EmbeddableAnnotation)) {
          val embeddablePiiElements = classSym.typeOfThis.members.collect {
            case field if getMemberType(field).contains(HasSensitiveField) =>
              getPIIDetailsForSymbol(field, None)
          }.toSeq

          embeddables += (fullClassName -> EmbeddableBaseMetaData(
            fullClassName,
            packageName,
            isTrait,
            isObject,
            parentNames,
            embeddablePiiElements))

        } else {
          events += (fullClassName -> EventBaseMetaData(fullClassName, packageName, isTrait, isObject, parentNames))
        }

        if (isMeta && isStorable) exportCataloguingMetadata(extractMetaParams(classSym, Meta))
      }

      Metadatas(entities.toMap, storables.toMap, embeddables.toMap, events.toMap, metas.toMap)
    }

    private def isValidPackageMeta(parents: Seq[Seq[Symbol]]): Boolean = {
      parents.flatten.contains(OwnershipMetadata.tpe.typeSymbol)
    }

    private def isValidMeta(parentTypes: Seq[Seq[Symbol]]): Boolean = {
      // a catalog alone is valid but will throw at runtime if no owner was assigned
      !(parentTypes.size == 1 && parentTypes.flatten.contains(OwnershipMetadata.tpe.typeSymbol))
    }

    val classes = new mutable.ListBuffer[MemberDef]
    val packageObjects = Map.newBuilder[String, CatalogingMetadata]
    def extractMetaParams(sym: Symbol, metaType: MetaType): CatalogingMetadata = {
      // this is the main extractor of fqcn for classes related to cataloguing
      // gives you both owner and catalog fqcn to be stored in the meta.json for runtime reflection
      val metaSym = sym.getAnnotation(MetaAnnotation).get.tree
      val parentTypes: Seq[Seq[Symbol]] = metaSym match {
        case Apply(_, args) => args.map(s => s.symbol.tpe.parents.map(_.typeSymbol))
        case _              => Seq.empty[Seq[Symbol]]
      }

      if (metaType.equals(PackageMeta) && !isValidPackageMeta(parentTypes))
        alarm(OptimusErrors.INCORRECT_PACKAGE_META_DEFINITION, sym.pos, sym.fullName)

      if (metaType.equals(Meta) && !isValidMeta(parentTypes))
        alarm(OptimusErrors.OWNER_ONLY_META_DEFINITION, sym.pos, sym.fullName)

      val metaMap = Map.newBuilder[ClassSymbol, String]
      metaSym match {
        case Apply(_, args) =>
          args foreach {
            case sel @ Select(MetaFieldPackage(_), _) if sel.symbol.isModule =>
              if (sel.symbol.typeSignature.baseClasses.contains(OwnershipMetadata)) {
                metaMap += (OwnershipMetadata -> sel.symbol.fullName)
              } else if (sel.symbol.typeSignature.baseClasses.contains(DalMetadata)) {
                metaMap += (DalMetadata -> sel.symbol.fullName)
              }
            case other =>
              alarm(OptimusErrors.INCORRECT_META_FIELDS, other.pos, other.symbol.fullName)
          }
        case other => alarm(OptimusErrors.INCORRECT_META_FIELDS, other.pos, other.symbol.fullName)
      }
      val mapResult = metaMap.result()
      CatalogingMetadata(
        mapResult.getOrElse(OwnershipMetadata, ""),
        mapResult.getOrElse(DalMetadata, "")
      )
    }

    def collectPackageMeta(pkgName: String, impl: Template): (String, CatalogingMetadata) = {
      // we are using 'find' here as the scala package object is indistinguishable from the actual package where the
      // annotation is located, we don't care which ones we use to get to the annotation as they are both
      // using the same path (ie a.b.c)
      val symPkgMeta: Option[Tree] = impl.find(e => e.hasSymbolField && e.symbol.hasAnnotation(MetaAnnotation))
      symPkgMeta match {
        case Some(e) =>
          val metaParams = extractMetaParams(e.symbol, PackageMeta)
          (pkgName, metaParams)
        case None => ("", CatalogingMetadata("", ""))
      }
    }

    def apply0(unit: CompilationUnit): Unit = gen(unit.body)

    def gen(tree: Tree): Unit = {
      def isSymbolContainsMetadata(symbol: Symbol) = {
        symbol.hasAnnotation(MetaAnnotation) ||
        symbol.hasAnnotation(EntityAnnotation) ||
        symbol.hasAnnotation(EmbeddableAnnotation) ||
        symbol.hasAnnotation(EventAnnotation)
      }

      tree match {
        case PackageDef(_, stats) => stats foreach gen
        case cd @ ClassDef(mods, name, tparams, impl) if isSymbolContainsMetadata(cd.symbol) =>
          classes += cd
          impl foreach gen
        case md @ ModuleDef(mods, name, impl) if isSymbolContainsMetadata(md.symbol) =>
          classes += md
          impl foreach gen
        case pko @ ClassDef(_, _, _, impl) if pko.symbol.isPackageObjectClass =>
          packageObjects += collectPackageMeta(pko.symbol.fullName.stripSuffix(".package"), impl)
        case _ => ()
      }
    }

    def buildPackageMetaMap(packages: Map[String, CatalogingMetadata]): Map[String, MetaBaseMetaData] = {
      packages.foldLeft(Map.empty[String, MetaBaseMetaData]) { case (packageMetaMap, (pkg, meta)) =>
        val pkgWtDelm = pkg.concat(OptimusExportInfoComponent.PackageMetaAnnotationSuffix) // delimiter
        packageMetaMap + (pkg ->
          MetaBaseMetaData(
            pkgWtDelm,
            pkg,
            meta.owner,
            meta.catalog,
            isEntity = false,
            isStorable = false,
            isObject = false,
            isTrait = false,
            List.empty[String]))
      }
    }

    override def run() = {
      if (diagnostics) reporter.echo(NoPosition, s"OptimusExportInfo.run for $projOutputDir")

      echoPhaseSummary(this)
      val units = currentRun.units
      while (units.hasNext)
        applyPhase(units.next())

      // here we'll take all of the package objects and create a new packageMeta, Map[String, MetaBaseMetaData]
      val packages = packageObjects.result().filterNot { case (name, _) => name.isBlank }
      val packageMeta: Map[String, MetaBaseMetaData] =
        buildPackageMetaMap(packages)
      val metadatas: Metadatas = buildMetadataMap(classes.toList)

      updateMetaDataFile(metadatas.entities, entityMetaDataFileName)
      updateMetaDataFile(metadatas.embeddables, embeddableMetaDataFileName)
      updateMetaDataFile(metadatas.events, eventMetaDataFileName)
      updateMetaDataFile(metadatas.storables, storedEntityMetaDataFileName)
      updateMetaDataFile(metadatas.metas ++ packageMeta, metaSquaredDataFileName)
    }

    private def isManuallyAddedFile(f: Path): Boolean = {
      val fullPath = f.toFile.toString
      // Deliberately organise conditions in such verbose way for readability
      (fullPath.endsWith(MetaDataFiles.eventMetaDataFileName) && fullPath.contains("dal_client")) ||
      (fullPath.endsWith(MetaDataFiles.entityMetaDataFileName) && fullPath.contains("core")) ||
      (fullPath.endsWith(MetaDataFiles.storedEntityMetaDataFileName) && fullPath.contains("core"))
    }

    private def updateMetaDataFile(newMetadata: Map[String, BaseMetaData], fileName: String): Unit = {
      import optimus.config.spray.json._
      import optimus.platform.metadatas.internal.MetaJsonProtocol._

      try {
        if (diagnostics)
          reporter.echo(NoPosition, s"updateMetaDataFile called: $projOutputDir/$fileName $newMetadata")

        // read metadatas from existing json
        // the job has already started before optimus_adjustast phase asynchronously
        val existingMetadata = workers.existingMetadata(fileName)
        // remove metadata for non-existent classes and update metadata of modified classes within this compilation run
        val filteredExisting = existingMetadata.filter { case (key, _) => workers.classNamesPresentInOutput(key) }
        val fullMetadata = filteredExisting ++ newMetadata
        if (diagnostics)
          reporter.echo(
            NoPosition,
            s"OptimusExportInfo.updateMetaDataFile $projOutputDir/$fileName: " +
              s"prev(${existingMetadata.size})=$existingMetadata, filtered(${filteredExisting.size})=$filteredExisting, " +
              s"res(${fullMetadata.size})=$fullMetadata"
          )
        // Persist updated metadata info into target file. Note that if was no meta data previously and now then we do
        // nothing, but if there was metadata previously and none now we'll need to overwrite the old file with empty data
        if (fullMetadata.nonEmpty || existingMetadata.nonEmpty) {
          val metadataSortedByClassName =
            fullMetadata.toList.sortBy { case (name, _) => name }.map { case (_, meta) => meta }
          val content = metadataSortedByClassName.toJson.prettyPrint.getBytes(MetaDataFiles.fileCharset)
          plugin.addAdditionalOutput(fileName, content)
        }
      } catch {
        case t: Throwable =>
          val message =
            s"OptimusExportInfo.updateMetaDataFile $projOutputDir/$fileName caught exception $t when processing"
          if (diagnostics) reporter.echo(NoPosition, message)
          throw new IllegalStateException(message, t)
      }
    }
  }

  private class MetadataConcurrentWorkers(scalacOutput: Path, classpath: String) {
    import scala.concurrent.ExecutionContext.Implicits.{global => ece}

    val isToJar: Boolean = scalacOutput.getFileName.toString.endsWith(".jar")

    val outputDir: Path =
      if (isToJar) scalacOutput.resolveSibling(scalacOutput.getFileName.toString.stripSuffix(".jar")) else scalacOutput

    private def traverseOutput(outputRoot: Path): (Set[String], Map[String, Map[String, BaseMetaData]]) = {
      val metadataWorkers = MetaDataFiles.metadataFileNames.map { fileName =>
        Future(
          fileName -> {
            val file = outputRoot.resolve(fileName)
            if (Files.exists(file)) loadExistingMetadatas(fileName, Files.readAllBytes(file))
            else Map.empty[String, BaseMetaData]
          }
        )
      }

      val buffer = mutable.HashSet[String]()
      Files.walkFileTree(
        outputRoot,
        new SimpleFileVisitor[Path] {
          @nowarn(
            "msg=10500 scala.collection.JavaConverters"
          ) // cannot use jdk.CollectionConverters in compiler plugins
          override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
            if (file.toString.endsWith(".class")) {
              import scala.collection.JavaConverters._
              val fullClassName =
                outputRoot.relativize(file).iterator().asScala.mkString(".").stripSuffix(".class")
              buffer += fullClassName
            }
            FileVisitResult.CONTINUE
          }
        }
      )

      val existingClassNames = buffer.toSet
      (existingClassNames, Await.result(Future.sequence(metadataWorkers), Duration.Inf).toMap)
    }

    private def traverseJarEntries(): (Set[String], Map[String, Map[String, BaseMetaData]]) = {
      // Zinc is adding previous jar as first entry on classpath
      val pathStr = classpath.indexOf(File.pathSeparatorChar) match {
        case -1 => classpath
        case n  => classpath.substring(0, n)
      }
      val path = Paths.get(pathStr)
      if (!path.getFileName.toString.contains("prev-jar") || !Files.exists(path))
        (Set.empty[String], MetaDataFiles.metadataFileNames.map(n => n -> Map.empty[String, BaseMetaData]).toMap)
      else {
        val jarFile = FileSystems.newFileSystem(path, null.asInstanceOf[ClassLoader])
        try traverseOutput(jarFile.getPath("/"))
        finally jarFile.close()
      }
    }

    /**
     * Traverse all classes under output directory and extract names
     */
    private lazy val fileWalker: Future[(Set[String], Map[String, Map[String, BaseMetaData]])] = Future {
      if (isToJar) traverseJarEntries() else traverseOutput(scalacOutput)
    }

    lazy val (classNamesPresentInOutput, existingMetadata) =
      waitForResult(fileWalker, "scan of existing class and json files")

    private def loadExistingMetadatas(fileName: String, bytes: Array[Byte]): Map[String, BaseMetaData] = {
      import optimus.platform.metadatas.internal.MetaJsonProtocol._
      try {
        // read metadata from existing json files
        val content = new String(bytes, MetaDataFiles.fileCharset)
        val json = JsonParser(content)
        val metas = fileName match {
          case MetaDataFiles.entityMetaDataFileName       => json.convertTo[List[EntityBaseMetaData]]
          case MetaDataFiles.storedEntityMetaDataFileName => json.convertTo[List[EntityBaseMetaData]]
          case MetaDataFiles.embeddableMetaDataFileName   => json.convertTo[List[EmbeddableBaseMetaData]]
          case MetaDataFiles.eventMetaDataFileName        => json.convertTo[List[EventBaseMetaData]]
          case MetaDataFiles.metaSquaredDataFileName      => json.convertTo[List[MetaBaseMetaData]]
        }
        metas.iterator.map(md => md.fullClassName -> md).toMap
      } catch {
        // catch *any* Throwable and wrap it in an Exception so that it will propagate back when the Future is accessed,
        // otherwise the Throwable from this background thread will be lost and the compiler can hang due to
        // incomplete Future.
        case t: Throwable =>
          throw new IllegalStateException("OptimusExportInfo cannot complete loading metadata files", t)
      }
    }

    @tailrec
    private def waitForResult[T](future: Future[T], reason: String, retries: Int = 10): T = {
      try {
        Await.result(future, 1 minute)
      } catch {
        case t: TimeoutException =>
          if (retries > 0) {
            if (diagnostics)
              reporter.echo(
                NoPosition,
                s"OptimusExportInfo.MetadataConcurrentWorkers: Waiting for $reason to finish ($retries tries left)")
            waitForResult(future, reason, retries - 1)
          } else throw t
      }
    }
  }
}

object OptimusExportInfoComponent {
  val PackageMetaAnnotationSuffix = "."
  val InheritedMetaAnnotationSuffix = "###"
}
