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
package optimus.buildtool.format
package app

import optimus.buildtool.config.JavacConfiguration

// usage: FilterJavaRuntimeOptions -XX:+PrintGCDetails -D11+:--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ...
object FilterJavaRuntimeOptions extends App {
  val targetVersion :: opts = args.toList

  val filtered = JavacConfiguration.filterJavaRuntimeOptions(targetVersion, opts, log = Console.err.println(_))

  println(filtered mkString " ")
}
