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
package optimus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class FieldInjector extends ClassVisitor implements Opcodes {
  public static final class InjectedField {
    public final String name;
    public final String descOrNull; // if null, the type will be inferred from getter or setter
    public final String getter; // name of a method that returns the field. null == no getter
    public final String setter; // name of method that sets the field. null == no setter
    public final int access; // field access flags, default to private | transient

    public InjectedField(String name, String descOrNull, String getter, String setter) {
      // by default, injected fields are private and transient.
      this(name, descOrNull, getter, setter, ACC_PRIVATE | ACC_TRANSIENT);
    }

    public InjectedField(String name, String descOrNull, String getter, String setter, int access) {
      this.name = name;
      this.descOrNull = descOrNull;
      this.getter = getter;
      this.setter = setter;
      this.access = access;
    }
  }

  private abstract class Accessor {
    final InjectedField field;
    final String method;
    private boolean written;

    private Accessor(String method, InjectedField field) {
      this.method = method;
      this.field = field;
    }

    protected abstract String typ();

    @Override
    public String toString() {
      return String.format(
          "%s(field=%s, type=%s, method=%s%s)",
          typ(),
          field.name,
          fieldDescriptors.getOrDefault(field, "<unknown>"),
          method,
          written ? ", written" : "");
    }

    void visit(MethodVisitor mv, String desc) {
      if (written) throw new IllegalStateException("wrote accessor " + method + " already!");
      written = true;
      visitCode(mv, desc);
    }

    protected abstract void visitCode(MethodVisitor mv, String desc);

    /** Generate the accessor if there isn't a prototype for it already. */
    abstract void visitMethod();
  }

  private void throwNoTemplateException(InjectedField field) {
    // When using inferred field type, we need at least one pre-defined setter or getter so that we
    // can correctly infer the type for the field. This gets called if we never encountered a
    // pre-defined accessors at all once we hit visitEnd().

    // returns one of "getter x", "setter y", "getter x or setter y"
    var accessorString =
        (field.getter == null ? "" : "getter " + field.getter)
            + (field.setter == null
                ? ""
                : (field.getter == null ? "" : " or ") + "setter " + field.setter);
    throw new RuntimeException(
        "Injected field "
            + field.name
            + "'s type should be inferred from "
            + accessorString
            + " but no matching method was found in class "
            + className);
  }

  private final class Getter extends Accessor {
    Getter(String name, InjectedField field) {
      super(name, field);
    }

    @Override
    protected String typ() {
      return "Getter";
    }

    protected void visitCode(MethodVisitor mv, String desc) {
      Type returnType;
      String fieldDesc;
      {
        returnType = Type.getReturnType(desc);
        var returnDesc = returnType.getDescriptor();
        fieldDesc = fieldDescriptors.computeIfAbsent(field, k -> returnDesc);
        if (!fieldDesc.equals(returnDesc))
          throw new IllegalStateException(
              this + ": return type " + desc + "doesn't match field type " + fieldDesc);
        if (Type.getArgumentCount(desc) != 0)
          throw new IllegalStateException(this + ": should have zero arguments, got " + desc);
      }

      mv.visitCode();

      var l0 = new Label();
      mv.visitLabel(l0);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitFieldInsn(GETFIELD, className, field.name, fieldDesc);
      mv.visitInsn(returnType.getOpcode(IRETURN));

      var l1 = new Label();
      mv.visitLabel(l1);
      mv.visitLocalVariable("this", "L" + className + ";", null, l0, l1, 0);
      mv.visitMaxs(2, 1);
      mv.visitEnd();
    }

    void visitMethod() {
      var fieldDesc = fieldDescriptors.get(field);
      if (fieldDesc == null) throwNoTemplateException(field);
      var getterDesc = String.format("()%s", fieldDesc);
      FieldInjector.this.visitMethod(ACC_PUBLIC, method, getterDesc, null, null);
    }
  }

  private final class Setter extends Accessor {
    Setter(String name, InjectedField field) {
      super(name, field);
    }

    @Override
    protected String typ() {
      return "Setter";
    }

    protected void visitCode(MethodVisitor mv, String desc) {

      Type argType;
      String fieldDesc;
      {
        Type[] args = Type.getArgumentTypes(desc);
        if (args.length != 1)
          throw new IllegalStateException(
              this + ": should have a single argument but found " + desc);
        argType = args[0];
        String argDesc = argType.getDescriptor();
        fieldDesc = fieldDescriptors.computeIfAbsent(field, k -> argDesc);

        if (!argDesc.equals(fieldDesc))
          throw new IllegalStateException(
              this + ": argument should be of type " + fieldDesc + " but found " + argDesc);

        var returnType = Type.getReturnType(desc);
        if (!returnType.equals(Type.VOID_TYPE))
          throw new IllegalStateException(
              this + ": should return void but got " + returnType); // todo : Unit?
      }

      mv.visitCode();

      var l0 = new Label();
      mv.visitLabel(l0);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitVarInsn(argType.getOpcode(ILOAD), 1);
      mv.visitFieldInsn(PUTFIELD, className, field.name, fieldDesc);

      var l1 = new Label();
      mv.visitLabel(l1);
      mv.visitInsn(RETURN);

      var l2 = new Label();
      mv.visitLabel(l2);
      mv.visitLocalVariable("this", "L" + className + ";", null, l0, l2, 0);
      mv.visitLocalVariable("v", fieldDesc, null, l0, l2, 1);
      mv.visitMaxs(3, 3);
      mv.visitEnd();
    }

    void visitMethod() {
      var fieldDesc = fieldDescriptors.get(field);
      if (fieldDesc == null) throwNoTemplateException(field);
      var setterDesc = String.format("(%s)%s", fieldDesc, Type.VOID_TYPE);
      FieldInjector.this.visitMethod(ACC_PUBLIC, method, setterDesc, null, null);
    }
  }

  private String className;
  private boolean skip;

  private final String implInterface;
  private final List<InjectedField> fields;
  private final Map<String, Accessor> accessors;
  // field descriptors are inferred from accessors or passed in explicitly
  private final Map<InjectedField, String> fieldDescriptors;

  /**
   * Create a new transformer that injects fields and accessors.
   *
   * <p>Transformed classes will have implInterface added to their list of interfaces. That new
   * interface provides a convenient place to put setters and getters if needed, but will also act
   * as a marker for the transformation.
   *
   * @param fields the fields to inject
   * @param implInterface interface added to transformed classes, can be null
   * @param cv delegating class visitor
   */
  public FieldInjector(List<InjectedField> fields, String implInterface, ClassVisitor cv) {
    super(ASM9, cv);
    this.fields = List.copyOf(fields);
    this.accessors = new HashMap<>();
    this.fieldDescriptors = new HashMap<>();
    this.implInterface = implInterface;
    Consumer<Accessor> insertAccessor =
        (Accessor a) -> {
          if (a == null) return;
          var existing = accessors.put(a.method, a);
          if (existing != null)
            throw new IllegalStateException(
                String.format("Accessors %s and %s have the same name", a, existing));
        };

    var names = new HashSet<String>();

    for (var f : fields) {
      if (!names.add(f.name))
        throw new IllegalStateException("Two injected fields share name " + f.name);
      if (f.descOrNull != null) fieldDescriptors.put(f, f.descOrNull);
      insertAccessor.accept((f.getter == null) ? null : new Getter(f.getter, f));
      insertAccessor.accept((f.setter == null) ? null : new Setter(f.setter, f));
    }
  }

  public void visit(
      int version,
      int access,
      String name,
      String signature,
      String superName,
      String[] interfaces) {
    this.className = name;
    skip = ((access & ACC_INTERFACE) == ACC_INTERFACE);

    String[] newInterfaces;
    if (implInterface == null || skip) {
      newInterfaces = interfaces;
    } else if (interfaces == null) {
      newInterfaces = new String[] {implInterface};
    } else {
      newInterfaces = Arrays.copyOf(interfaces, interfaces.length + 1);
      newInterfaces[interfaces.length] = implInterface;
    }

    cv.visit(version, access, name, signature, superName, newInterfaces);
  }

  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    // If there already is a setter or a getter, we replace its code with the code we need for the
    // injected field.
    MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
    if (skip) return mv;

    var acc = accessors.get(name);
    if (acc == null) return mv;
    else {
      acc.visit(mv, desc);
      return null;
    }
  }

  public void visitEnd() {
    if (skip) return;

    for (var acc : accessors.values()) {
      if (!acc.written) {
        acc.visitMethod();
        if (!acc.written) throw new RuntimeException("did not write accessor for " + acc);
      }
    }

    for (var f : fields) {
      var desc = fieldDescriptors.get(f);
      if (desc == null)
        throw new IllegalStateException(String.format("Field type wasn't inferred for %s", f));
      cv.visitField(f.access, f.name, desc, null, null);
    }

    cv.visitEnd();
  }

  /**
   * Creates a field injector for a list of properties. For each property `x`, the field injector
   * will add a transient, private field `_x` and will rewrite existing getters `getx` and setters
   * `setx`. The type of the field will be inferred from the type of the getter or setter.
   */
  public static FieldInjector forProperties(
      List<String> properties, String markerInterface, ClassVisitor cv) {
    var fields =
        properties.stream()
            .map(
                name ->
                    new InjectedField(
                        "_" + name, null, "get" + name, "set" + name, ACC_TRANSIENT | ACC_PRIVATE))
            .collect(Collectors.toList());

    return new FieldInjector(fields, markerInterface, cv);
  }
}
