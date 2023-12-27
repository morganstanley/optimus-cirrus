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
package optimus.buildtool.compilers.cpp

import optimus.buildtool.compilers.cpp.gcc.GccCompiler
import optimus.buildtool.compilers.cpp.gcc.GccLinker
import optimus.buildtool.compilers.cpp.vc.VcCompiler
import optimus.buildtool.compilers.cpp.vc.VcLinker
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.ScopeId

trait CppCompilerFactory {
  def newCompiler(scopeId: ScopeId, toolchain: CppToolchain): CppFileCompiler
  def newLinker(scopeId: ScopeId, toolchain: CppToolchain): CppLinker
}

class CppCompilerFactoryImpl extends CppCompilerFactory {
  override def newCompiler(scopeId: ScopeId, toolchain: CppToolchain): CppFileCompiler = toolchain.tpe match {
    case "vc"  => new VcCompiler(scopeId, toolchain)
    case "gcc" => new GccCompiler(scopeId, toolchain)
    case x     => throw new IllegalArgumentException(s"Unknown toolchain type: $x")
  }

  override def newLinker(scopeId: ScopeId, toolchain: CppToolchain): CppLinker = toolchain.tpe match {
    case "vc"  => new VcLinker(scopeId, toolchain)
    case "gcc" => new GccLinker(scopeId, toolchain)
    case x     => throw new IllegalArgumentException(s"Unknown toolchain type: $x")
  }
}
