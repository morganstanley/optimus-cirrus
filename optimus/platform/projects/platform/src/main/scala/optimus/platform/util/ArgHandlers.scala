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
package optimus.platform.util

import com.ms.zookeeper.clientutils.ZkEnv
import org.apache.zookeeper.ZooDefs.Perms
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter

/*
 * Please put new handlers in [[optimus.utils.app.Args4jOptionHandlers]] instead if possible.
 *
 * This file should only contains handlers for types not accessible from the utils project.
 */

final class ZkEnvHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[ZkEnv])
    extends OneArgumentOptionHandler[ZkEnv](parser, option, setter) {
  override def parse(arg: String): ZkEnv = ZkEnv.valueOf(arg)
}

final class ZkPermsHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Int])
    extends OneArgumentOptionHandler[Int](parser, option, setter) {
  override def parse(arg: String): Int =
    arg.toCharArray
      .map {
        case 'A' => Perms.ADMIN
        case 'C' => Perms.CREATE
        case 'D' => Perms.DELETE
        case 'R' => Perms.READ
        case 'W' => Perms.WRITE
        case c   => throw new IllegalArgumentException(s"Unexpected permission: $c")
      }
      .foldLeft(0)(_ | _)
}
