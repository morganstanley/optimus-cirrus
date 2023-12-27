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
package optimus.gittools

import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.NonReactiveDirectoryFactory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Module
import optimus.buildtool.format.WorkspaceStructure
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.GitUtils
import optimus.platform.ChromeBrowserConfiguration
import optimus.platform._
import optimus.scalacompat.collection._
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.ResetCommand.ResetType
import org.eclipse.jgit.api.errors.RefAlreadyExistsException
import org.eclipse.jgit.diff.DiffEntry
import org.eclipse.jgit.diff.DiffEntry.ChangeType
import org.eclipse.jgit.revwalk.RevCommit

import scala.jdk.CollectionConverters._
import scala.collection.SortedMap
import scala.collection.mutable
import scala.io.StdIn

/**
 * A handy tool for splitting large commits into multiple PRs.
 *
 * Before running this tool, create a single commit containing all of the changes on a suitably-named branch. Ensure
 * that your workspace is clean and that the HEAD commit has a sensible one-line message.
 *
 * Run this tool from the workspace src/ root, and it will analyse the HEAD commit and batch it into N batches of files,
 * based on the --maxFiles and --maxOwners parameters. The tool tries (not very hard) to keep files contained in
 * projects with the same owner group (as per bundles.obt) together. The idea is that projects with the same owner group
 * are likely to have similar autobuild review and test rules.
 *
 * You can then hit 'y' and <enter> to generate and push a branch for each batch. Each branch is named foo-N (where foo
 * was the original branch name), and is created from HEAD^1. A single commit containing one batch of changes is added
 * to each branch, and they are all pushed to origin. The commit message will be the top line of the original HEAD
 * commit message with "(part N of M)" appended.
 *
 * You can then hit 'y' and <enter> again to open the Create PR page in BitBucket for each of the branches, and from
 * that point onwards you can follow the usual PR workflow.
 *
 * If you want to re-run the split, you can use --force to force the branch creation and push operations to overwrite
 * existing branches. If you're brave you can also specify --yes to override the confirmation prompts.
 */
//noinspection ScalaDocParserErrorInspection,ScalaDocUnclosedTagWithoutParser
class CommitSplitCmdLine extends OptimusAppCmdLine {
  import org.kohsuke.{args4j => args}
  @args.Option(
    name = "--srcDir",
    required = false,
    usage = "Workspace source root (defaults to the current directory)"
  )
  val srcDir: String = ""

  @args.Option(
    name = "--maxFiles",
    required = false,
    usage = "Maximum files per PR"
  )
  val maxFiles: Int = 500

  @args.Option(
    name = "--maxOwners",
    required = false,
    usage = "Maximum module owner groups per PR"
  )
  val maxOwners: Int = 5

  @args.Option(
    name = "--force",
    required = false,
    usage = "Force Git operations"
  )
  val force: Boolean = false

  @args.Option(
    name = "--yes",
    required = false,
    usage = "Answer 'y' to any 'do you want to continue' prompts"
  )
  val yes: Boolean = false
}

object CommitSplit extends OptimusApp[CommitSplitCmdLine] {
  private val log = getLogger(this)

  private implicit val moduleOrdering: Ordering[Module] = Ordering.by((m: Module) => m.id.toString)
  private val NoModuleId = ModuleId("n/a", "n/a", "n/a")
  private val NoModule = Module(NoModuleId, RelativePath.empty, "n/a", "n/a", Nil, -1)

  @entersGraph def run(): Unit = {
    val srcPath = getSrcPath
    val moduleMetadata = loadModuleMetadata(srcPath)

    verifyGitExe(srcPath)
    val (git, gitLog) = getGit(srcPath)

    log.info("Reading git log...")
    val recentCommits = git.log.setMaxCount(3).call.asScala.toList
    val headCommit :: previousCommit :: _ = recentCommits

    val originalBranch = git.getRepository.getBranch
    log.info(s"On branch $originalBranch")

    log.info(s"Will split HEAD commit [${headCommit.getId.getName}] ${headCommit.getShortMessage}")
    log.info("Analysing diff...")
    val diff = gitLog.diff(previousCommit.getId.getName, headCommit.getId.getName)

    val diffsByModule: SortedMap[Module, Seq[DiffEntry]] = sorted(
      diff
        .groupBy { d =>
          val path =
            if (d.getChangeType == ChangeType.DELETE) RelativePath(d.getOldPath) else RelativePath(d.getNewPath)
          moduleMetadata.moduleForFile(path).getOrElse(NoModule)
        })

    val diffsInMetaBundles: SortedMap[MetaBundle, SortedMap[Module, Seq[DiffEntry]]] =
      sorted(diffsByModule.groupBy(_._1.id.metaBundle))

    val diffs: SortedMap[MetaBundle, SortedMap[String, SortedMap[Module, Seq[DiffEntry]]]] =
      diffsInMetaBundles.mapValuesNow(xs => sorted(xs.groupBy(_._1.owningGroup)))

    log.info("======")
    log.info(s"Breakdown of ${diff.size} modified files by meta.bundle, owner group and module:")

    for ((metaBundle, diffsByGroupAndModule) <- diffs) {
      log.info(s"Meta and bundle $metaBundle:")
      for ((group, map) <- diffsByGroupAndModule) {
        log.info(s"  Owner group $group:")
        for ((module, diffs) <- map) {
          log.info(s"    Module ${module.id}: ${diffs.size} file(s)")
        }
      }
    }

    val batches: List[List[(Module, DiffEntry)]] = createBatches(diffs)

    log.info("======")
    log.info(s"Will create ${batches.size} branch(es)")
    batches.foreach { b =>
      val metaBundle = b.map(_._1.id.metaBundle).distinct.single
      val groups = b.map(_._1.owningGroup).distinct
      val modules = b.map(_._1).distinct
      log.info(
        s"[$metaBundle]: ${b.size} file(s) from ${modules.size} module(s) from " +
          s"${groups.size} group(s) [${groups.mkString(", ")}]")
    }

    log.info("======")
    checkContinue(s"to create ${batches.size} branches")

    val createUrls =
      try {
        batches.zipWithIndex.map { case (batch, idx) =>
          val num = idx + 1
          val branch = s"$originalBranch-$num"
          log.info(s"Creating branch $branch...")
          createBranch(git, previousCommit, branch)
          log.info(s"  Applying changes...")
          applyChanges(git, headCommit, batch)
          log.info(s"  Committing...")
          git.commit
            .setNoVerify(true)
            .setMessage(s"${headCommit.getShortMessage} (part $num of ${batches.size})")
            .call()
          log.info(s"  Pushing...")
          // we use real git executable for push because I can't get the spnego authentication to work with jgit
          val pushArgs = Seq("push", "--set-upstream") ++ (if (cmdLine.force) Seq("--force") else Nil)
          val createUrls = runGitExe(srcPath, pushArgs ++ Seq("origin", branch)) {
            // grab the URL to create the PR
            case s if s.contains("/pull-requests?create") =>
              s.substring(s.indexOf("http"))
          }
          createUrls.single
        }
      } finally {
        log.info(s"Resetting and checking out original branch...")
        git.reset.setMode(ResetType.HARD).call()
        git.checkout.setName(originalBranch).call()
      }
    log.info("======")
    log.info("Create Branch URLs:")
    createUrls.foreach(url => log.info(s"  > $url"))

    checkContinue("to open the above Create Branch URLs in Chrome")
    createUrls.foreach(url => Runtime.getRuntime.exec(Array(ChromeBrowserConfiguration.chromeExeLocation, url)))
  }

  private def checkContinue(msg: String): Unit = {
    if (!cmdLine.yes) {
      log.info(s"Hit 'y' and <enter> $msg, or hit any other key to exit")
      if (StdIn.readLine().trim != "y") {
        log.info("Exiting as requested")
        exit(None, 0)
      }
    }
  }
  private def createBranch(git: Git, startingCommit: RevCommit, branch: String) = {
    val create = git.checkout.setCreateBranch(true).setName(branch).setStartPoint(startingCommit)
    try create.call()
    catch {
      case e: RefAlreadyExistsException =>
        if (cmdLine.force) {
          log.info("  Branch already exists. Re-creating...")
          git.branchDelete().setBranchNames(branch).setForce(true).call()
          create.call()
        } else {
          log.error("Branch already exists. Re-run with --force to overwrite it.")
          throw e
        }
    }
  }

  private def applyChanges(git: Git, sourceCommit: RevCommit, batch: List[(Module, DiffEntry)]): Unit = {
    val diffs = batch.map(_._2)
    val newOrModified = diffs.filter(d => d.getChangeType != ChangeType.DELETE).map(_.getNewPath)
    git.checkout.setStartPoint(sourceCommit).addPaths(newOrModified.asJava).call()
    log.info(s"  Adding files to index...")
    val add = git.add
    newOrModified.foreach(add.addFilepattern)
    add.call()
    val deletedOrRenamed = diffs
      .filter(d => d.getChangeType == ChangeType.DELETE || d.getChangeType == ChangeType.RENAME)
      .map(_.getOldPath)
    if (deletedOrRenamed.nonEmpty) {
      log.info(s"  Applying deletions...")
      val rm = git.rm
      deletedOrRenamed.foreach(rm.addFilepattern)
      rm.call()
    }
  }

  private def createBatches(diffs: SortedMap[MetaBundle, SortedMap[String, SortedMap[Module, Seq[DiffEntry]]]])
      : List[List[(Module, DiffEntry)]] = {
    val batch: mutable.ListBuffer[(Module, DiffEntry)] = mutable.ListBuffer.empty
    var batches: mutable.ListBuffer[List[(Module, DiffEntry)]] = mutable.ListBuffer.empty
    val groupsInBatch: mutable.Set[String] = mutable.Set.empty

    def cutBatch(): Unit = {
      if (batch.nonEmpty) {
        batches += batch.result()
        groupsInBatch.clear()
        batch.clear()
      }
    }

    for ((_, diffsByGroupAndModule) <- diffs) {
      for ((group, map) <- diffsByGroupAndModule) {
        for ((module, diffs) <- map) {
          for (d <- diffs) {
            groupsInBatch += group // add here because cutBatch clears it
            batch += ((module, d))
            if (batch.size >= cmdLine.maxFiles) cutBatch() // honour maxFiles
          }
        }
        if (groupsInBatch.size >= cmdLine.maxOwners) cutBatch() // honour maxOwners
      }
      cutBatch() // don't allow more than 1 meta.bundle per batch
    }

    cutBatch()
    batches.result()
  }

  private def sorted[K: Ordering, V](m: Map[K, V]): SortedMap[K, V] = SortedMap(m.toSeq: _*)

  private def getSrcPath: Directory = {
    // note that user.dir is PWD, not home directory
    val srcPath = if (cmdLine.srcDir.nonEmpty) Paths.get(cmdLine.srcDir) else Paths.get(System.getProperty("user.dir"))
    require(
      srcPath.getFileName.toString == "src",
      s"$srcPath did not end in 'src'. Either run this tool from a codetree src directory, or specify the src directory using --srcDir"
    )
    require(
      Files.isDirectory(srcPath),
      s"$srcPath did not exist or was not a directory. Either run this tool from a codetree src directory, or specify the src directory using --srcDir"
    )
    Directory(srcPath)
  }

  private def loadModuleMetadata(srcPath: Directory): ModuleMetadata = {
    log.info("Loading module structure...")
    val loader = ObtFile.FilesystemLoader(srcPath)
    val structureResult = WorkspaceStructure.loadModuleStructure(srcPath.parent.name, loader)
    val errors = structureResult.problems.filter(_.isError)
    if (errors.nonEmpty) {
      errors.foreach(p => log.error(p.toString))
      throw new IllegalArgumentException("Unable to resolve OBT module metadata")
    } else {
      val structure = structureResult.get
      new ModuleMetadata(structure)
    }
  }

  @async private def getGit(srcPath: Directory): (Git, GitLog) = {
    log.info("Initializing jgit...")
    val gitUtils = GitUtils
      .find(srcPath, NonReactiveDirectoryFactory)
      .getOrElse(throw new IllegalArgumentException(s"Unable to find .git directory in $srcPath"))
    val git = gitUtils.git
    val gitLog = GitLog(gitUtils, srcPath, commitLength = 3)
    log.info("Checking for local changes...")
    if (!git.status.call.isClean) {
      throw new IllegalStateException("You have local changes. Please commit or revert them first")
    }
    (git, gitLog)
  }

  def verifyGitExe(srcPath: Directory): Unit = {
    log.info("Verifying that git executable works...")
    val version = runGitExe(srcPath, Seq("--version")) {
      case s if s.contains("git version") => s
    }

    if (version.isEmpty) throw new IllegalStateException(s"git --version didn't seem to reply with 'git version'")
  }

  def runGitExe[T](srcPath: Directory, args: Seq[String])(f: PartialFunction[String, T]): Seq[T] = {
    val cmd = "git" +: args
    val p = new ProcessBuilder(cmd: _*).redirectErrorStream(true).directory(srcPath.path.toFile).start()
    val in = new BufferedReader(new InputStreamReader(p.getInputStream))
    val fnOutput = mutable.ListBuffer[T]()
    val allOutput = mutable.ListBuffer[String]()
    val exitCode =
      try {
        var l = in.readLine()
        while (l ne null) {
          allOutput += l
          if (f.isDefinedAt(l)) fnOutput += f(l)
          l = in.readLine()
        }
        p.waitFor()
      } finally in.close()

    if (exitCode != 0) {
      // noinspection ConvertibleToMethodValue
      allOutput.result().foreach(log.error(_))
      throw new IllegalStateException(s"'${cmd.mkString(" ")}' returned exit code $exitCode")
    }
    fnOutput.result()
  }
}

private class ModuleMetadata(structure: WorkspaceStructure) {
  private val modulesByPath = structure.modules.values.map(p => (p.path.parent, p)).toMap
  private val modulePathLengths = modulesByPath.keys.map(_.path.iterator().asScala.length).toSeq.distinct.sorted.reverse

  /** finds the module which matches the longest prefix of the path, or else None */
  def moduleForFile(path: RelativePath): Option[Module] = {
    modulePathLengths.flatMap { l =>
      path.subPath(0, l).flatMap(modulesByPath.get)
    }.headOption
  }
}
