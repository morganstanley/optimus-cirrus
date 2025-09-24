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
package optimus.debug;

import static optimus.debug.InstrumentationConfig.CACHED_VALUE_TYPE;
import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;
import static optimus.debug.InstrumentationConfig.THROWABLE;
import static optimus.debug.InstrumentationConfig.THROWABLE_TYPE;
import optimus.debug.InstrumentationConfig.*;
import optimus.DynamicClassLoader;
import optimus.graph.rtverifier.CallWithArgsGenerator;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

class InstrumentationInjectorMethodVisitor extends CommonAdapter {
  private final MethodPatch patch;
  private String callWithArgsCtorDescriptor;
  private String callWithArgsType;
  private final Label tryBlockStart = new Label();
  private final Label tryBlockEnd = new Label();
  private final Label catchBlockStart = new Label();
  private final Label catchBlockEnd = new Label();

  InstrumentationInjectorMethodVisitor(
      MethodPatch patch, MethodVisitor mv, int access, String name, String descriptor) {
    super(mv, access, name, descriptor);
    this.patch = patch;
  }

  private void injectMethodPrefix() {
    if (patch.cacheInField != null) ifNotZeroReturn(patch, patch.cacheInField);

    if (patch.prefix == null && !patch.localValueIsCallWithArgs)
      return; // prefix is implied for localValueIsCallWithArgs

    MethodRef prefix = patch.prefix;

    if (patch.passLocalValue) visitLocalValueStart();

    var descriptor = "(";
    if (patch.prefixWithID) descriptor += loadMethodID(patch);

    if (patch.prefixWithThis) descriptor += loadThisOrNull(patch);

    if (patch.prefixWithArgs) descriptor += loadArgsInlineOrAsArray(patch);

    if (patch.passLocalValue || patch.storeToField != null) descriptor += ")" + OBJECT_DESC;
    else descriptor += ")V";

    if (patch.localValueIsCallWithArgs) {
      mv.visitTypeInsn(NEW, callWithArgsType);
      dup();
      loadThisIfNonStatic();
      loadArgs();
      mv.visitMethodInsn(
          INVOKESPECIAL, callWithArgsType, "<init>", callWithArgsCtorDescriptor, false);
    }

    if (prefix != null) {
      // If descriptor was supplied just use that
      if (prefix.descriptor != null) descriptor = prefix.descriptor;

      mv.visitMethodInsn(INVOKESTATIC, prefix.cls, prefix.method, descriptor, false);
    }

    if (patch.passLocalValue) {
      storeLocalValue();
    } else if (patch.storeToField != null) {
      // TODO (OPTIMUS-76886): This should call the setter provided by FieldInjector instead.
      loadThis();
      swap();
      mv.visitFieldInsn(
          PUTFIELD, patch.from.cls, patch.storeToField.name, patch.storeToField.descOrNull);
    }

    if (patch.checkAndReturn) {
      loadLocalValue();
      mv.visitFieldInsn(GETFIELD, CACHED_VALUE_TYPE, "hasResult", "Z");
      var continueLabel = new Label();
      mv.visitJumpInsn(IFEQ, continueLabel);
      loadLocalValue();
      mv.visitFieldInsn(GETFIELD, CACHED_VALUE_TYPE, "result", OBJECT_DESC);
      mv.visitTypeInsn(CHECKCAST, Type.getReturnType(methodDesc).getInternalName());
      mv.visitInsn(ARETURN);
      mv.visitLabel(continueLabel);
    }

    if (patch.wrapWithTryCatch) {
      mv.visitLabel(tryBlockStart);
    }
  }

  @Override
  public void visitCode() {
    if (patch.passLocalValue && !patch.localValueIsCallWithArgs) {
      var localValueType =
          patch.prefix.descriptor != null
              ? Type.getMethodType(patch.prefix.descriptor).getReturnType()
              : OBJECT_TYPE;
      setLocalValueTypeAndDesc(localValueType);
    }

    if (patch.localValueIsCallWithArgs) {
      // we generate a CallWithArgs instance...
      Type thisOwner = Type.getObjectType(patch.from.cls);
      callWithArgsType = CallWithArgsGenerator.generateClassName(getName());
      byte[] newBytes =
          CallWithArgsGenerator.create(
              methodAccess,
              callWithArgsType,
              thisOwner,
              getArgumentTypes(),
              getReturnType(),
              getName());
      DynamicClassLoader.loadClassInCurrentClassLoader(newBytes);
      callWithArgsCtorDescriptor =
          CallWithArgsGenerator.getCtrDescriptor(methodAccess, thisOwner, getArgumentTypes());
      // ...and we pass our CallWithArgs instance to the suffix calls
      setLocalValueTypeAndDesc(Type.getObjectType(InstrumentationConfig.CWA));
    }
    super.visitCode();
    injectMethodPrefix();
  }

  @Override
  protected void onMethodExit(int opcode) {
    if (opcode == ATHROW && (patch.noArgumentBoxing || patch.wrapWithTryCatch))
      return; // do not generate exit call at the point of exception throw

    if (patch.cacheInField != null) {
      dup();
      loadThis();
      swap();
      // TODO (OPTIMUS-76886): This should call the getter provided by FieldInjector instead.
      mv.visitFieldInsn(
          PUTFIELD, patch.from.cls, patch.cacheInField.name, patch.cacheInField.descOrNull);
    }

    if (patch.suffix == null) return;

    var descriptor = "(";

    if (patch.suffixWithReturnValue)
      descriptor += dupReturnValueOrNullForVoid(opcode, !patch.noArgumentBoxing);

    if (patch.passLocalValue) descriptor += loadLocalValue();

    if (patch.suffixWithID) descriptor += loadMethodID(patch);

    if (patch.suffixWithThis) descriptor += loadThisOrNull(patch);

    if (patch.suffixWithArgs) descriptor += loadArgsInlineOrAsArray(patch);

    if (patch.suffixReplacesReturnValue) {
      if (patch.noArgumentBoxing)
        descriptor += ")" + Type.getReturnType(methodDesc).getDescriptor();
      else descriptor += ")" + OBJECT_DESC;
    } else descriptor += ")V";

    // If descriptor was supplied just use that
    if (patch.suffix.descriptor != null) descriptor = patch.suffix.descriptor;

    mv.visitMethodInsn(INVOKESTATIC, patch.suffix.cls, patch.suffix.method, descriptor, false);

    // n.b. unbox does a checkcast if the return type is non-primitive, so this handles both
    // primitive and non-primitive cases
    if (patch.suffixReplacesReturnValue && !patch.noArgumentBoxing)
      unbox(Type.getReturnType(methodDesc));
  }

  @Override
  public void visitMaxs(int maxStack, int maxLocals) {
    if (patch.wrapWithTryCatch) {
      visitLabel(tryBlockEnd);
      mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, THROWABLE);
      mv.visitInsn(NOP);
      visitLabel(catchBlockStart);

      var suffixOnException = patch.suffixOnException;
      if (suffixOnException != null) {
        dup();
        var descriptor =
            "(" + THROWABLE_TYPE.getDescriptor() + loadLocalValueIfRequested(patch) + ")V";
        mv.visitMethodInsn(
            INVOKESTATIC, suffixOnException.cls, suffixOnException.method, descriptor, false);
      }
      throwException();
      visitLabel(catchBlockEnd);
    }

    if (patch.passLocalValue) visitLocalValueEnd();

    super.visitMaxs(maxStack, maxLocals);
  }
}
