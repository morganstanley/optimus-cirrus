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
package optimus.buildtool.builders.postinstallers.uploaders

import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader._
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.DirectoryAsset
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.OsUtils

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Paths
import scala.collection.immutable.Seq

sealed abstract class UploadLocation {

  def target: Directory
  protected def location: String
  override def toString: String = location

  def cmds(source: Asset, format: UploadFormat): Seq[Seq[String]]

  protected def untarCmd(tar: FileAsset): Seq[String] =
    Seq(
      "tar",
      "xzfp",
      str(tar),
      "-C",
      str(target),
      "--skip-old-files",
      "--warning=no-timestamp"
    )

  protected def unzipCmd(zip: FileAsset): Seq[String] =
    Seq("unzip", "-n", str(zip), "-d", str(target))

  protected def copyCmd(zip: FileAsset): Seq[String] =
    if (OsUtils.isWindows) Seq("cmd", "/c", "copy", osStr(zip), osStr(target))
    else Seq("cp", str(zip), str(target))

  // Windows adds a trailing "/" to one-level deep network paths (eg. //foo/bar) - we want to trim these
  protected def str(a: Asset): String = {
    val str = a.pathString
    if (str.endsWith("/")) str.dropRight(1) else str
  }

  private def osStr(a: Asset): String = a.path.toString

  // rsync doesn't like windows paths beginning with a drive letter, and directories should be suffixed with a "/"
  // to ensure they're copied at the correct level
  protected def rsyncPath(a: Asset): String = {
    val str = a.pathString.replaceFirst("^([A-Z]):", "/cygdrive/$1")
    a match {
      case _: DirectoryAsset if !str.endsWith("/") => s"$str/"
      case _                                       => str
    }
  }
}

object UploadLocation {

  final case class Local(target: Directory, decompressAfterUpload: Boolean) extends UploadLocation {
    override protected val location: String = str(target)

    def cmds(source: Asset, format: UploadFormat): Seq[Seq[String]] = (source, format) match {
      case (d: DirectoryAsset, Raw) =>
        Seq(Seq("rsync", "--partial", "--recursive", "--compress", rsyncPath(d), rsyncPath(target)))
      case (tar: FileAsset, Tar) =>
        if (decompressAfterUpload) Seq(untarCmd(tar))
        else Seq(copyCmd(tar))
      case (zip: FileAsset, Zip) =>
        if (decompressAfterUpload) Seq(unzipCmd(zip))
        else Seq(copyCmd(zip))
      case x => throw new UnsupportedOperationException(s"Invalid upload combination: $x")
    }
  }

  final case class Remote(host: String, target: Directory, decompressAfterUpload: Boolean) extends UploadLocation {
    override protected val location: String = s"$host:${str(target)}"

    // We assume the remote host is always linux. Don't use "//tmp" here because Paths.get will fail on windows.
    private val Tmp = Directory(Paths.get("//tmp/obt-upload"))
    private val AND = Seq("&&")

    // remotely, we need to (for files):
    // if decompressAfterUpload: first copy the zip to /tmp, them unzip it to NFS
    // else: copy the zip to NFS
    // for directories: copy the directory to NFS
    override def cmds(source: Asset, format: UploadFormat): Seq[Seq[String]] = (source, format) match {
      case (d: DirectoryAsset, Raw) =>
        Seq(
          overSsh { makeDir(target) },
          copy(d, target)
        )
      case (tar: FileAsset, Tar) =>
        if (decompressAfterUpload) {
          val tempFile = Tmp.resolveFile(tar.name)
          Seq(
            overSsh { makeDir(Tmp) },
            copy(tar, Tmp),
            overSsh { untarCmd(tempFile) ++ AND ++ delete(tempFile) }
          )
        } else {
          Seq(
            overSsh { makeDir(target) },
            copy(tar, target)
          )
        }
      case (zip: FileAsset, Zip) =>
        if (decompressAfterUpload) {
          val tempFile = Tmp.resolveFile(zip.name)
          Seq(
            overSsh { makeDir(Tmp) },
            copy(zip, Tmp),
            overSsh { unzipCmd(tempFile) ++ AND ++ delete(tempFile) }
          )
        } else {
          Seq(
            overSsh { makeDir(target) },
            copy(zip, target)
          )
        }
      case x => throw new UnsupportedOperationException(s"Invalid upload combination: $x")
    }

    private def makeDir(target: Directory): Seq[String] =
      Seq("mkdir", "-p", str(target))

    private def copy(source: Asset, target: Directory): Seq[String] = {
      val extraArgs = source match {
        case _: FileAsset      => Nil
        case _: DirectoryAsset => Seq("--recursive", "--compress")
      }
      Seq("rsync", "--partial") ++ extraArgs ++ Seq("-e", "ssh", rsyncPath(source), s"$host:${rsyncPath(target)}")
    }

    private def delete(file: FileAsset): Seq[String] = Seq("rm", str(file))

    private def overSsh(baseCmd: Seq[String]): Seq[String] = Seq("ssh", host, baseCmd.mkString(" "))
  }

  def apply(text: String, decompressAfterUpload: Boolean, fs: FileSystem = FileSystems.getDefault): UploadLocation = {
    text.split(":").filter(_.nonEmpty) match {
      case Array(volume, _) if volume.length == 1 =>
        // assuming we have a local Windows path here
        // since in Windows volumes are identified by a single digit letter
        Local(Directory(fs.getPath(text)), decompressAfterUpload)
      case Array(h, d) => Remote(h, Directory(fs.getPath(d)), decompressAfterUpload)
      case Array(d)    => Local(Directory(fs.getPath(d)), decompressAfterUpload)
      case _           => throw new IllegalArgumentException(s"Invalid upload location $text")
    }
  }
}
