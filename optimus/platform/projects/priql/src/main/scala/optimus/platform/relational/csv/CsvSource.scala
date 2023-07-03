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
package optimus.platform.relational.csv

import java.io._
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicBoolean

import optimus.platform._
import optimus.platform.relational.RelationalException
import optimus.platform.relational.tree._
import optimus.platform.storable.{EntityCompanionBase, Entity}

sealed abstract class AbstractCsvSource[RowType: TypeInfo](
    val csvProvider: ReaderProvider,
    /**
     * when hasHeader is true, the first line would be its header.
     */
    val hasHeader: Boolean,
    val timeZone: ZoneId) {

  if (csvProvider == null) throw new IllegalArgumentException("Does not provide CSV Data Provider")

  def reader: Reader = csvProvider.reader

  private[optimus] val rowType = typeInfo[RowType]
  private[optimus] val rowTypeClass: Class[RowType] = rowType.runtimeClass

}

class DynamicCsvSource(
    csvProvider: ReaderProvider,
    hasHeader: Boolean,
    timeZone: ZoneId,
    val convertor: CsvFieldConvertor = null)
    extends AbstractCsvSource[DynamicObject](csvProvider, hasHeader, timeZone) {
  def this(csvBytes: Array[Byte], hasHeader: Boolean, timeZone: ZoneId, convertor: CsvFieldConvertor) =
    this(new ByteArrayReaderProvider(csvBytes), hasHeader, timeZone, convertor)
  def this(csv: String, hasHeader: Boolean, timeZone: ZoneId, convertor: CsvFieldConvertor) =
    this(new ByteArrayReaderProvider(csv.getBytes), hasHeader, timeZone, convertor)
  def this(csvFile: File, hasHeader: Boolean, timeZone: ZoneId, convertor: CsvFieldConvertor) =
    this(new FileReaderProvider(csvFile), hasHeader, timeZone, convertor)
  def this(input: InputStream, hasHeader: Boolean, timeZone: ZoneId, convertor: CsvFieldConvertor) =
    this(new InputStreamReaderProvider(input), hasHeader, timeZone, convertor)

  def this(input: InputStream, hasHeader: Boolean) = this(new InputStreamReaderProvider(input), hasHeader, null, null)
  def this(csvBytes: Array[Byte], hasHeader: Boolean) =
    this(new ByteArrayReaderProvider(csvBytes), hasHeader, null, null)
  def this(csv: String, hasHeader: Boolean) = this(new ByteArrayReaderProvider(csv.getBytes), hasHeader, null, null)
  def this(csvFile: File, hasHeader: Boolean) = this(new FileReaderProvider(csvFile), hasHeader, null, null)

  def this(input: InputStream, hasHeader: Boolean, timeZone: ZoneId) =
    this(new InputStreamReaderProvider(input), hasHeader, timeZone, null)
  def this(csvBytes: Array[Byte], hasHeader: Boolean, timeZone: ZoneId) =
    this(new ByteArrayReaderProvider(csvBytes), hasHeader, timeZone, null)
  def this(csv: String, hasHeader: Boolean, timeZone: ZoneId) =
    this(new ByteArrayReaderProvider(csv.getBytes), hasHeader, timeZone, null)
  def this(csvFile: File, hasHeader: Boolean, timeZone: ZoneId) =
    this(new FileReaderProvider(csvFile), hasHeader, timeZone, null)

  def this(input: InputStream, hasHeader: Boolean, convertor: CsvFieldConvertor) =
    this(new InputStreamReaderProvider(input), hasHeader, null, convertor)
  def this(csvBytes: Array[Byte], hasHeader: Boolean, convertor: CsvFieldConvertor) =
    this(new ByteArrayReaderProvider(csvBytes), hasHeader, null, convertor)
  def this(csv: String, hasHeader: Boolean, convertor: CsvFieldConvertor) =
    this(new ByteArrayReaderProvider(csv.getBytes), hasHeader, null, convertor)
  def this(csvFile: File, hasHeader: Boolean, convertor: CsvFieldConvertor) =
    this(new FileReaderProvider(csvFile), hasHeader, null, convertor)
}

class CsvSource[RowType <: Entity: TypeInfo](
    val entity: EntityCompanionBase[RowType],
    csvProvider: ReaderProvider,
    hasHeader: Boolean,
    timeZone: ZoneId)
    extends AbstractCsvSource[RowType](csvProvider, hasHeader, timeZone) {
  def this(entity: EntityCompanionBase[RowType], csvBytes: Array[Byte], hasHeader: Boolean, timeZone: ZoneId) =
    this(entity, new ByteArrayReaderProvider(csvBytes), hasHeader, timeZone)
  def this(entity: EntityCompanionBase[RowType], csv: String, hasHeader: Boolean, timeZone: ZoneId) =
    this(entity, new ByteArrayReaderProvider(csv.getBytes), hasHeader, timeZone)
  def this(entity: EntityCompanionBase[RowType], csvFile: File, hasHeader: Boolean, timeZone: ZoneId) =
    this(entity, new FileReaderProvider(csvFile), hasHeader, timeZone)
  def this(entity: EntityCompanionBase[RowType], input: InputStream, hasHeader: Boolean, timeZone: ZoneId) =
    this(entity, new InputStreamReaderProvider(input), hasHeader, timeZone)

  def this(entity: EntityCompanionBase[RowType], input: InputStream, hasHeader: Boolean) =
    this(entity, new InputStreamReaderProvider(input), hasHeader, null)
  def this(entity: EntityCompanionBase[RowType], csvBytes: Array[Byte], hasHeader: Boolean) =
    this(entity, new ByteArrayReaderProvider(csvBytes), hasHeader, null)
  def this(entity: EntityCompanionBase[RowType], csv: String, hasHeader: Boolean) =
    this(entity, new ByteArrayReaderProvider(csv.getBytes), hasHeader, null)
  def this(entity: EntityCompanionBase[RowType], csvFile: File, hasHeader: Boolean) =
    this(entity, new FileReaderProvider(csvFile), hasHeader, null)

  if (entity == null) throw new IllegalArgumentException("Does not provide entity")
}

/**
 * Return a new reader to csv data. It will throw exception if the underlying input stream does not support repeatable
 * read.
 */
trait ReaderProvider {
  def reader: Reader
}

class ByteArrayReaderProvider(val bytes: Array[Byte]) extends ReaderProvider {
  if (bytes == null) throw new IllegalArgumentException("data is null")
  def reader: Reader = new InputStreamReader(new ByteArrayInputStream(bytes))
}

class FileReaderProvider(val file: File) extends ReaderProvider {
  if (file == null) throw new IllegalArgumentException("file is null")
  def reader: Reader = new InputStreamReader(new FileInputStream(file))
}

class InputStreamReaderProvider(val input: InputStream) extends ReaderProvider {

  if (input == null) throw new IllegalArgumentException("input is null")
  private val hasReaded = new AtomicBoolean(false)

  def reader: Reader = {
    if (hasReaded.compareAndSet(false, true))
      new InputStreamReader(input)
    else
      throw new RelationalException("The underlying InputStream has been read.")
  }
}
