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
package optimus.buildtool.app

import optimus.buildtool.cache.silverking.SilverKingCacheProvider
import optimus.buildtool.compilers.zinc.RootLocatorReader
import optimus.buildtool.files.NonReactiveDirectoryFactory
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.GitUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._
import optimus.platform.dal.config.DalEnv

class CatchUpAppCmdLine extends OptimusAppCmdLine with GitCmdLine with SkCmdLine with WorkspaceCmdLine {
  import org.kohsuke.{args4j => args}

  @args.Option(name = "--gitRecurse", required = false, usage = "Recursively walk the full catchup history")
  val gitRecurse = false

  @args.Argument(metaVar = "branch", usage = "Find cache hits in the history of this ref", required = true)
  val ref: String = ""
}

/**
 * Prints the best commits for the specified branch that have full builds in remote cache and then exits. This is used
 * by "strato catchup".
 */
object CatchUpApp extends OptimusApp[CatchUpAppCmdLine] {
  override val dalLocation = DalEnv("none")
  @entersGraph override def run(): Unit = {
    if (!new CatchUpApp(cmdLine).run(cmdLine.ref, cmdLine.gitRecurse)) setReturnCode(1)
  }
}

class CatchUpApp(cmdLine: GitCmdLine with SkCmdLine with WorkspaceCmdLine) extends util.Log {
  import cmdLine._

  // noinspection ConvertibleToMethodValue
  @async def run(ref: String, recurse: Boolean): Boolean = {
    catchup(ref, recurse) match {
      case Some(lines) => lines.foreach(log.info(_)); true
      case None        => false
    }
  }

  @async def catchup(ref: String, recurse: Boolean): Option[Seq[String]] = {
    val srcRoot = WorkspaceSourceRoot(workspaceSourceRoot)
    val outputDir = cmdLine.outputDir.asDirectory.getOrElse(workspaceRoot.resolveDir("build_obt"))
    val pathBuilder = CompilePathBuilder(outputDir)
    val skCaches = SilverKingCacheProvider(cmdLine, cmdLine.artifactVersion, pathBuilder)
    for {
      git <- GitUtils.find(srcRoot, NonReactiveDirectoryFactory).orTap {
        log.error(s"No git found in $srcRoot -- are you in a workspace?")
      }
      gitLog = GitLog(git, workspaceSourceRoot, cmdLine.gitFilterRe, cmdLine.gitLength)
      rootLocatorCache <- skCaches.remoteRootLocatorCache.orTap {
        log.error(s"Cannot find root-locator cache at $silverKing")
      }
    } yield {
      new RootLocatorReader(gitLog, rootLocatorCache.store).findRootLocatorsAsStrings(ref, recurse)
    }
  }
}
