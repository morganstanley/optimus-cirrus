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
package optimus.dsi.partitioning

import optimus.platform.dsi.bitemporal.DSIException
import optimus.platform.dsi.bitemporal.ReadOnlyCommand

/**
 * Exceptions relating to partitions
 */
class UndefinedPartitionException(partition: Partition, msg: String = "")
    extends DSIException(
      s"DAL request attempted to access an undefined partition: $partition. ${if (msg.nonEmpty) s"Detailed message = ${msg}"
        else ""}")
class MultiPartitionWriteException(partitions: Seq[Partition])
    extends DSIException(
      s"DAL request attempted to write to multiple partitions: ${partitions.mkString("(", ",", ")")}")
class MultiPartitionReadException(cmds: Seq[ReadOnlyCommand], partitions: Seq[Partition])
    extends DSIException(
      s"DAL request attempted to perform a single command that read from  multiple partitions: ${partitions
          .mkString("(", ",", ")")}, $cmds")
class MultiPartitionConfigException(cmds: Seq[ReadOnlyCommand], partitions: Seq[Partition])
    extends DSIException("Multipartition reads attempted to run sequentially")
class PartitionConfigurationException(msg: String) extends DSIException(msg)
class SecureEntityAccessException(entities: Seq[String])
    extends DSIException(
      s"DAL request attempted to access secured entities over open socket connection: Entities(s) ${entities.toString()}")
