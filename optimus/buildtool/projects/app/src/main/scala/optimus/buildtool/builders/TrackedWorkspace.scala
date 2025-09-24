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
package optimus.buildtool.builders

// N.B. we're using Java Futures because that's what the BSP library wants, and it's easier not to fight with it
import optimus.buildtool.app.MischiefOptions
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.rubbish.RubbishTidyer
import optimus.buildtool.rubbish.StoredArtifacts
import optimus.buildtool.trace.FindArtifacts
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ObtTraceListener
import optimus.buildtool.trace.ScanFilesystem
import optimus.buildtool.trace.TidyRubbish
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.Utils
import optimus.core.needsPluginAlwaysAutoAsyncArgs
import optimus.graph.CancellationScope
import optimus.graph.tracking.DependencyTracker
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.util.Log

import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object TrackedWorkspace {
  private class Promise[A]() {
    val future = new CompletableFuture[A]()
    def complete(t: Try[A]): Unit = t match {
      case Success(a) => future.complete(a)
      case Failure(x) => future.completeExceptionally(x)
    }
  }

  def completed[A](a: A): CompletableFuture[A] = CompletableFuture.completedFuture(a)
  def combine[A, B, C](af: CompletableFuture[A], bf: CompletableFuture[B])(
      f: (A, B) => CompletableFuture[C]
  ): CompletableFuture[C] = af.thenCompose(a => bf.thenCompose(b => f(a, b)))
}

class TrackedWorkspace(
    tracker: DependencyTracker,
    directoryFactory: DirectoryFactory,
    rubbishTidyer: Option[RubbishTidyer],
    mischiefOptions: MischiefOptions
) extends Log {
  import TrackedWorkspace._
  import Utils._

  def rescan(cancelScope: CancellationScope, listener: ObtTraceListener): CompletableFuture[Boolean] = {
    val tweaks = run(cancelScope, listener) {
      val (scanTime, scanTweaks) = ObtTrace.traceTask(RootScopeId, ScanFilesystem) {
        AdvancedUtils.timed(directoryFactory.getTweaksAndReset())
      }
      val scanDetails = directoryFactory.scanDetails
      val scanMsg =
        f"Scanned filesystem (${scanTweaks.size}/${scanDetails.watchedPaths.size}%,d directories changed, ${scanDetails.scannedFiles}%,d files scanned) in ${scanTime / 1.0e6}%,.1fms"
      ObtTrace.info(scanMsg)
      log.info(scanMsg)

      val mischiefTweaks = mischiefOptions.configAsTweaks()

      val externalTweaks = Seq(
        // We always assume that mutable external artifacts may have changed. Usually there aren't any so this
        // costs us nothing (and if there are any we just pay the cost to rehash them)
        ExternalClassFileArtifact.mutableExternalArtifactState.tweakVersion(),
        GitLog.gitStatusState.tweakVersion(),
      )

      val allTweaks = externalTweaks ++ scanTweaks ++ mischiefTweaks
      val workspaceChanged = scanTweaks.nonEmpty
      (allTweaks, workspaceChanged)
    }

    tweaks.thenCompose { case (ts, workspaceChanged) => addResolvedTweaks(ts).thenApply(_ => workspaceChanged) }
  }

  def previousArtifacts(
      cancelScope: CancellationScope,
      listener: ObtTraceListener): CompletableFuture[StoredArtifacts] =
    run(cancelScope, listener) {
      rubbishTidyer match {
        case Some(t) =>
          val (findTime, artifacts) = ObtTrace.traceTask(RootScopeId, FindArtifacts) {
            AdvancedUtils.timed(t.storedArtifacts)
          }
          val totalSize = artifacts.artifacts.map(_.size).sum
          val findMsg =
            f"Found ${artifacts.artifacts.size}%,d stored artifacts (${bytesToString(
                totalSize)} total size, ${bytesToString(artifacts.toTidyBytes)} to be deleted) in ${findTime / 1.0e6}%,.1fms"
          ObtTrace.info(findMsg)
          log.info(findMsg)
          artifacts
        case None => StoredArtifacts.empty
      }
    }

  def tidyRubbish(
      cancelScope: CancellationScope,
      listener: ObtTraceListener
  )(
      artifacts: StoredArtifacts,
      excludedPaths: Seq[Path]
  ): CompletableFuture[Unit] = {
    run(cancelScope, listener) {
      rubbishTidyer match {
        case Some(t) =>
          val (tidyTime, rubbish) = ObtTrace.traceTask(RootScopeId, TidyRubbish) {
            AdvancedUtils.timed(t.tidy(artifacts, excludedPaths))
          }
          val rubbishMsg =
            if (rubbish.sizeBytes > 0)
              f"Deleted ${rubbish.artifacts.size}%,d pieces of rubbish (total size ${bytesToString(
                  rubbish.sizeBytes)}) in ${tidyTime / 1.0e6}%,.1fms"
            else "No rubbish deleted"
          ObtTrace.info(rubbishMsg)
          log.info(rubbishMsg)
          rubbish.tweaks
        case None => Nil
      }
    }.thenCompose(addResolvedTweaks)
  }

  def addTweaks(cancelScope: CancellationScope, listener: ObtTraceListener)(
      tweaks: => Seq[Tweak]
  ): CompletableFuture[Unit] = run(cancelScope, listener)(tweaks).thenCompose(addResolvedTweaks)
  // noinspection ScalaUnusedSymbol
  def addTweaks$NF(cancelScope: CancellationScope, listener: ObtTraceListener)(
      tweaks: AsyncFunction0[Seq[Tweak]]
  ) = run(cancelScope, listener)(tweaks()).thenCompose(addResolvedTweaks)

  @closuresEnterGraph
  def run[A](cancelScope: CancellationScope, listener: ObtTraceListener)(task: => A): CompletableFuture[A] =
    eval(withContext(cancelScope, listener)(task))

  private def addResolvedTweaks(tweaks: Seq[Tweak]): CompletableFuture[Unit] =
    if (tweaks.nonEmpty) {
      val p = new Promise[Unit]()
      tracker.addTweaksAsync(tweaks, throwOnDuplicate = true, p.complete)
      p.future
    } else completed(())

  private def eval[A](f: => A): CompletableFuture[A] = {
    val p = new Promise[A]()
    tracker.executeEvaluateAsync(() => f, p.complete)
    p.future
  }

  @alwaysAutoAsyncArgs private def withContext[A](
      cancelScope: CancellationScope,
      listener: ObtTraceListener
  )(
      subtask: => A
  ): A = needsPluginAlwaysAutoAsyncArgs

  @entersGraph @closuresEnterGraph private def withContext$NF[A](
      cancelScope: CancellationScope,
      listener: ObtTraceListener
  )(
      subtask: NodeFunction0[A]
  ): A = asyncResult(cancelScope) {
    ObtTrace.withListener(listener) {
      track {
        subtask()
      }
    }
  }.value

}
