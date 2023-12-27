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
package optimus.buildtool.builders.postinstallers

import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

/**
 * A set of actions to run after files have been written as part of the install process.
 */
trait PostInstaller {

  /**
   * Post-install files for given scopes.
   */
  @async def postInstallFiles(ids: Set[ScopeId], files: Seq[FileAsset], successful: Boolean): Unit = ()

  /**
   * Post-install files that are not specific to a bundle or scope (such as testplans or transitive artifacts).
   */
  @async def postInstallFiles(files: Seq[FileAsset], successful: Boolean): Unit =
    postInstallFiles(ids = Set.empty, files, successful)

  /**
   * Completes the install.
   */
  @async def complete(successful: Boolean): Unit = ()

}

object PostInstaller {

  /** Null object for [[PostInstaller]]s. */
  val zero: PostInstaller = new PostInstaller {}

  private class MergingPostInstaller(installers: Seq[PostInstaller]) extends PostInstaller {
    @async override def postInstallFiles(ids: Set[ScopeId], files: Seq[FileAsset], successful: Boolean): Unit =
      installers.apar.foreach(_.postInstallFiles(ids, files, successful))
    @async override def postInstallFiles(files: Seq[FileAsset], successful: Boolean): Unit =
      installers.apar.foreach(_.postInstallFiles(files, successful))
    @async override def complete(successful: Boolean): Unit =
      installers.apar.foreach(_.complete(successful))
  }

  def merge(installers: Iterable[PostInstaller]): PostInstaller = installers.toIndexedSeq match {
    case Seq(i) => i
    case is     => new MergingPostInstaller(is)
  }
}
