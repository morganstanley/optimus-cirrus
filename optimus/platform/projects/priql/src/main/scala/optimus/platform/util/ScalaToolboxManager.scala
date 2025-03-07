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

import msjava.slf4jutils.scalalog._
import optimus.core.utils.RuntimeMirror
import optimus.platform.PackageAliases
import optimus.tools.scalacplugins.entity.EntityPlugin

import scala.reflect.runtime.universe._
import scala.tools.nsc.Global
import scala.tools.reflect.{ToolBox, ToolBoxError}
import scala.reflect.internal.Trees

private[optimus] /*[util]*/ class ScalaToolboxManager {
  import scala.reflect.internal.Internals

  private val log = getLogger[ScalaToolboxManager]
  private val mirror = RuntimeMirror.forClass(getClass)

  private val compilerOpts = {
    val plugins = ScalaCompilerUtils.entityJars
    val aliases = PackageAliases.aliasFlags.mkString(" ")
    s"-Xplugin-require:entity -Xplugin:$plugins $aliases"
  }

  // I've measured it to be quite a lot faster to re-use the same toolbox rather than creating a new one for each compilation
  private var toolbox = buildToolbox

  def buildToolbox = mirror.mkToolBox(options = compilerOpts)

  def resetToolbox() = { toolbox = buildToolbox }

  def eval[T](str: String): T = {
    eval(parse(str))
  }

  def parse(str: String): Tree = {
    toolbox.parse(str).asInstanceOf[Tree]
  }

  /**
   * compiles and evaluates the specified code using a cached toolbox instance
   */
  // we have to spilt this method
  def eval[T](tree: Tree): T = {
    try {
      // the toolbox doesn't apply pre-namer phases, so we apply them ourselves
      val hackedToolbox = toolbox.asInstanceOf[{
        val withCompilerApi: {
          def apply[K](f: {
            def compiler: Global {
              def compile(tree: Trees#Tree): () => Any
            }
            def importer: Internals#Importer
          } => K): K
        }
      }]
      hackedToolbox.withCompilerApi { case compilerApi =>
        val importer = compilerApi.importer
        val compilerTree = importer.importTree(tree.asInstanceOf[importer.from.Tree])
        val compiler = compilerApi.compiler
        val ep = new EntityPlugin(compiler)
        val transformedTree = ep.applyPreNamerTranforms(compilerTree.asInstanceOf[ep.global.Tree])
        compiler.compile(transformedTree)().asInstanceOf[T]
      }
    } catch {
      case e: ToolBoxError => log.error(s"Failed to compile code at runtime: $tree", e); throw e
    }
  }
}
