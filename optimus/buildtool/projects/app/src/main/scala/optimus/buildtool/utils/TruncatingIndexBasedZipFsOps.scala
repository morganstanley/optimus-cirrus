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
 *
 * This source file is a fork of code from Zinc, to which the following license
 * applies
 *
 * Zinc - The incremental compiler for Scala.
 * Copyright Scala Center, Lightbend, and Mark Harrah
 *
 * Licensed under Apache License 2.0
 * SPDX-License-Identifier: Apache-2.0
 *
 * See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */
package optimus.buildtool.utils

import sbt.internal.inc.zip.ZipCentralDir

import java.io.OutputStream
import java.lang.reflect.Field
import java.nio.file.Path
import scala.jdk.CollectionConverters._

/**
 * The concrete implementation of [[TruncatingIndexBasedZipOps]]
 * based on [[sbt.internal.inc.zip.ZipCentralDir]].
 */
object TruncatingIndexBasedZipFsOps extends TruncatingIndexBasedZipOps {
  override type CentralDir = ZipCentralDir
  override type Header = ZipCentralDir.Entry

  override protected def readCentralDir(path: Path): CentralDir = {
    new ZipCentralDir(path)
  }

  override protected def getCentralDirStart(centralDir: CentralDir): Long = {
    centralDir.getCentralDirStart
  }

  override protected def setCentralDirStart(centralDir: CentralDir, centralDirStart: Long): Unit = {
    centralDir.setCentralDirStart(centralDirStart)
  }

  override protected def getHeaders(centralDir: CentralDir): Seq[Header] = {
    centralDir.getHeaders.asScala.toVector
  }

  override protected def setHeaders(centralDir: CentralDir, headers: Seq[Header]): Unit = {
    centralDir.setHeaders(new java.util.ArrayList[Header](headers.asJava))
  }

  override protected def getFileName(header: Header): String =
    header.getName

  override protected def getFileOffset(header: Header): Long =
    header.getEntryOffset

  override protected def setFileOffset(header: Header, offset: Long): Unit =
    header.setEntryOffset(offset)

  override protected def getLastModifiedTime(header: Header): Long =
    header.getLastModifiedTime

  private def fieldAccess(fieldName: String): Field = {
    val field = classOf[Header].getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  private val mtime = fieldAccess("mtime")
  private val atime = fieldAccess("atime")
  private val ctime = fieldAccess("ctime")
  private val csize = fieldAccess("csize")

  protected def setLastModifiedTime(header: Header, value: Long): Unit = mtime.set(header, value)
  protected def setLastAccessedTime(header: Header, value: Long): Unit = atime.set(header, value)
  protected def setCreationTime(header: Header, value: Long): Unit = ctime.set(header, value)

  /**
   *  We need access to the compressed size from central directory header, because entry locator header
   *  usually is empty and the actual size is located in Data Descriptor.
   *  We need the compressed size before we start reading the file,
   *  as we don't know where Data Descriptor starts. This allow us to copy data without decompression.
   *
   *  OFFICIAL ZIP SPECIFICATION [[https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT]]
   *  SECTIONS 4.4.8 - 4.4.9
   */
  protected def getCompressedSize(header: Header): Long = csize.get(header).asInstanceOf[Long]

  override protected def writeCentralDir(centralDir: CentralDir, outputStream: OutputStream): Unit =
    centralDir.dump(outputStream)

}
