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
package optimus.platform.relational.asm

import org.objectweb.asm.Handle
import org.objectweb.asm.Opcodes
import optimus.platform.storable.Entity

trait ASMConstants {
  final val ClassfileVersion = Opcodes.V11

  val Predef = "scala/Predef"
  val Func = "optimus/platform/relational/asm/Func"
  val Closure = "optimus/platform/relational/asm/Closure"
  val WrappedArray = "scala/collection/mutable/WrappedArray"
  val ArraySeq = "scala/collection/mutable/ArraySeq"
  val ClosureCls = "optimus/platform/relational/asm/Closure"
  val EntityClass = classOf[Entity]
  val ObjectClass = classOf[Object]
  val BoxesRuntime = "scala/runtime/BoxesRunTime"
  val Object = "java/lang/Object"
  val Float = "java/lang/Float"
  val Double = "java/lang/Double"
  val Integer = "java/lang/Integer"
  val String = "java/lang/String"
  val Comparable = "java/lang/Comparable"
  val Exception = "java/lang/Exception"
  val Node = "optimus/graph/Node"
  val RelationKeyObject = "optimus/platform/RelationKey$"
  val ContinuationObject = "optimus/platform/relational/asm/Continuation$"
  val Function = "java/util/function/Function"
  val BiFunction = "java/util/function/BiFunction"
  val LambdaMetafactory = "java/lang/invoke/LambdaMetafactory"
  val MetafactorySignature =
    "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;"
  val AlreadyCompletedNode = "optimus/graph/AlreadyCompletedNode"
  val Ctor = "<init>"
  val FieldInst = "_keyImpl_row"
  val ConstsInst = "consts$instance"
  val LeftInst = "left$instance"
  val RightInst = "right$instance"
  val Class = "java/lang/Class"
  val Method = "java/lang/reflect/Method"
  val RelationalException = "optimus/platform/relational/RelationalException"
  val ProxyMarkerTrait = "optimus/platform/util/ProxyMarker$OptimusProxyMarkerTrait"
  val ASMGeneratorUtilsCls = "optimus/platform/relational/asm/ASMGeneratorUtils"
  val StringBuilder = "java/lang/StringBuilder"
  val CtorParamsField = "s$ctorParams"
  val OuterField = "s$outer"
  val AbstractDynamicObject = "optimus/platform/relational/AbstractDynamicObject"
  val DynamicObject = "optimus/platform/DynamicObject"
  val Sequence = "scala/collection/Seq"
  val ImmutableMap = "scala/collection/immutable/Map"
  val ScalaFunction0 = "scala/Function0"

  def mkBootstrapHandle =
    new Handle(Opcodes.H_INVOKESTATIC, LambdaMetafactory, "metafactory", MetafactorySignature, false)
}
