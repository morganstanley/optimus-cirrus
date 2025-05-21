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
package optimus.platform.pickling;

import static java.lang.invoke.MethodHandles.Lookup.ClassOption.STRONG;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import optimus.core.InternerHashEquals;
import optimus.platform.PluginHelpers;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ClassWriterEx;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.commons.TableSwitchGenerator;

/*
 * This class generates a class that extends SlottedBuffer. The generated class has a constructor
 * that takes the same number of arguments as the number of slots in the SlottedBuffer. The
 * generated class has a get method that can be used to get the value of a slot by index. The fields
 * that represent the slots are public and can be used to access the values directly in order
 * to avoid the boxing overhead of the get method.
 */
public class SlottedBufferClassGenerator implements Opcodes {
  private static final ConcurrentHashMap<String, SlottedBufferCtorHolder> classCache =
      new ConcurrentHashMap<>();

  private static final String MAP_BASE_CLASS_NAME = "optimus/platform/pickling/SlottedBufferAsMap";
  private static final String SEQ_BASE_CLASS_NAME = "optimus/platform/pickling/SlottedBufferAsSeq";
  private static final String GET_METHOD_NAME = "get";

  private static final String WRITE_METHOD_NAME = "write";

  @SuppressWarnings("unused") // Used by generated code.
  public static final Object ILLEGAL_INDEX = new Object();

  // Name of the above constant to be used in generating code.
  private static final String ILLEGAL_INDEX_NAME = "ILLEGAL_INDEX";

  private static final Type FIELD_PROTO_TYPE =
      Type.getType("Loptimus/platform/dsi/bitemporal/proto/Dsi/FieldProto;");
  private static final Type PROTO_PICKLE_SERIALIZER_TYPE =
      Type.getType("Loptimus/platform/dsi/bitemporal/proto/ProtoPickleSerializer;");

  private static final Map<Type, Method> PRIMITIVE_PROTOBUF_ACCESSOR_BY_TYPE =
      Map.of(
          Type.INT_TYPE,
          new Method("getIntValue", Type.getMethodDescriptor(Type.INT_TYPE, FIELD_PROTO_TYPE)),
          Type.DOUBLE_TYPE,
          new Method(
              "getDoubleValue", Type.getMethodDescriptor(Type.DOUBLE_TYPE, FIELD_PROTO_TYPE)),
          Type.BOOLEAN_TYPE,
          new Method("getBoolValue", Type.getMethodDescriptor(Type.BOOLEAN_TYPE, FIELD_PROTO_TYPE)),
          Type.CHAR_TYPE,
          new Method("getCharValue", Type.getMethodDescriptor(Type.CHAR_TYPE, FIELD_PROTO_TYPE)),
          Type.LONG_TYPE,
          new Method("getLongValue", Type.getMethodDescriptor(Type.LONG_TYPE, FIELD_PROTO_TYPE)),
          Type.SHORT_TYPE,
          new Method("getShortValue", Type.getMethodDescriptor(Type.SHORT_TYPE, FIELD_PROTO_TYPE)),
          Type.BYTE_TYPE,
          new Method("getByteValue", Type.getMethodDescriptor(Type.BYTE_TYPE, FIELD_PROTO_TYPE)));

  private static final Map<Type, Method> PRIMITIVE_STREAM_MUTATOR_BY_TYPE =
      Map.of(
          Type.INT_TYPE,
          new Method("writeInt", Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE)),
          Type.DOUBLE_TYPE,
          new Method("writeDouble", Type.getMethodDescriptor(Type.VOID_TYPE, Type.DOUBLE_TYPE)),
          Type.BOOLEAN_TYPE,
          new Method("writeBoolean", Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE)),
          Type.CHAR_TYPE,
          new Method("writeChar", Type.getMethodDescriptor(Type.VOID_TYPE, Type.CHAR_TYPE)),
          Type.LONG_TYPE,
          new Method("writeLong", Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE)),
          Type.SHORT_TYPE,
          new Method("writeShort", Type.getMethodDescriptor(Type.VOID_TYPE, Type.SHORT_TYPE)),
          Type.BYTE_TYPE,
          new Method("writeByte", Type.getMethodDescriptor(Type.VOID_TYPE, Type.BYTE_TYPE)));

  private static final Map<Type, Method> PRIMITIVE_STREAM_ACCESSOR_BY_TYPE =
      Map.of(
          Type.INT_TYPE,
          new Method("readInt", Type.getMethodDescriptor(Type.INT_TYPE)),
          Type.DOUBLE_TYPE,
          new Method("readDouble", Type.getMethodDescriptor(Type.DOUBLE_TYPE)),
          Type.BOOLEAN_TYPE,
          new Method("readBoolean", Type.getMethodDescriptor(Type.BOOLEAN_TYPE)),
          Type.CHAR_TYPE,
          new Method("readChar", Type.getMethodDescriptor(Type.CHAR_TYPE)),
          Type.LONG_TYPE,
          new Method("readLong", Type.getMethodDescriptor(Type.LONG_TYPE)),
          Type.SHORT_TYPE,
          new Method("readShort", Type.getMethodDescriptor(Type.SHORT_TYPE)),
          Type.BYTE_TYPE,
          new Method("readByte", Type.getMethodDescriptor(Type.BYTE_TYPE)));
  private static final String SB_CLASS_GENERATOR_CLASS =
      "optimus/platform/pickling/SlottedBufferClassGenerator";

  @SuppressWarnings("ClassEscapesDefinedScope") // Shape is private[platform] defined in Scala
  protected static SlottedBufferCtorHolder getOrResolveSBCtors(Shape shape) {
    return classCache.computeIfAbsent(shape.signature(), k -> create(shape));
  }

  private static SlottedBufferCtorHolder create(Shape shape) {
    var bytes = createClass(shape);
    MethodHandles.Lookup caller = MethodHandles.lookup();
    try {
      var cls = caller.defineHiddenClass(bytes, false, STRONG).lookupClass();
      MethodType mainCtorMethod;
      MethodType streamCtorMethod;
      Class<?> returnType;
      if (shape.isSeq()) {
        mainCtorMethod = MethodType.methodType(void.class, Object[].class);
        streamCtorMethod = MethodType.methodType(void.class, ObjectInput.class);
        returnType = SlottedBufferAsSeq.class;
      } else {
        mainCtorMethod = MethodType.methodType(void.class, Object[].class, Shape.class);
        streamCtorMethod = MethodType.methodType(void.class, ObjectInput.class, Shape.class);
        returnType = SlottedBufferAsMap.class;
      }
      var main =
          caller
              .findConstructor(cls, mainCtorMethod)
              .asType(mainCtorMethod.changeReturnType(returnType));
      var stream =
          caller
              .findConstructor(cls, streamCtorMethod)
              .asType(streamCtorMethod.changeReturnType(returnType));
      return new SlottedBufferCtorHolder(main, stream);
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("ClassEscapesDefinedScope") // Shape is private[platform] defined in Scala
  public static byte[] createClass(Shape shape) {
    var cw = new ClassWriterEx(ClassWriter.COMPUTE_FRAMES);
    String clsName = "optimus/platform/pickling/SB" + shape.signature();
    cw.visit(
        V11,
        ACC_PUBLIC | ACC_SUPER,
        clsName,
        null,
        shape.isSeq() ? SEQ_BASE_CLASS_NAME : MAP_BASE_CLASS_NAME,
        null);

    // Generate fields and get the types
    var types = new Type[shape.classes().length];
    for (int i = 0; i < types.length; ++i) {
      types[i] = Type.getType(shape.classes()[i]);
      // Fields will be named as _0, _1 etc.
      cw.writeField("_" + i, types[i]);
    }

    generateMainCtor(cw, clsName, types, shape.isSeq());

    generateStreamCtor(cw, clsName, types, shape.isSeq());

    generateGet(cw, clsName, types);

    generateArgsHash(cw, clsName, types);

    generateArgsEqual(cw, clsName, types);

    generateStreamWrite(cw, clsName, types);

    if (shape.isSeq()) {
      generateLength(cw, types.length);
      generateSignature(cw, shape.signature());
    }

    cw.visitEnd();
    return cw.toByteArray();
  }

  private static void generateMainCtor(
      ClassWriter cw, String clsName, Type[] types, Boolean asSeq) {
    String ctorDesc;
    if (asSeq)
      // SlottedBufferAsSeq doesn't need the Shape instance
      ctorDesc = Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object[].class));
    else
      ctorDesc =
          Type.getMethodDescriptor(
              Type.VOID_TYPE,
              Type.getType(Object[].class), // values
              Type.getType(Shape.class) // Shape
              );
    var mv = cw.visitMethod(ACC_PUBLIC, "<init>", ctorDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, "<init>", ctorDesc);
    gen.visitCode();
    gen.loadThis();
    if (asSeq) {
      gen.invokeConstructor(
          Type.getObjectType(SEQ_BASE_CLASS_NAME),
          new Method("<init>", Type.getMethodDescriptor(Type.VOID_TYPE)));
    } else {
      gen.loadArg(1); // Shape
      gen.invokeConstructor(
          Type.getObjectType(MAP_BASE_CLASS_NAME),
          new Method(
              "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Shape.class))));
    }
    for (int i = 0; i < types.length; ++i) {
      gen.loadThis();
      gen.loadArg(0); // values array
      gen.push(i);
      gen.arrayLoad(Type.getType(Object.class));
      Method method = PRIMITIVE_PROTOBUF_ACCESSOR_BY_TYPE.get(types[i]);
      if (null != method) {
        // [SEE_FIELDPROTO_FOR_PRIMITIVES]
        // Field is primitive type. We want to cast the value to FieldProto and call the
        // primitive accessor method to avoid boxing.
        gen.checkCast(FIELD_PROTO_TYPE);
        gen.invokeStatic(PROTO_PICKLE_SERIALIZER_TYPE, method);
      } else if (!asSeq) {
        // Field is Object type and this is a SlottedBufferAsMap. We want to go through
        // Shape.maybeIntern
        gen.loadArg(1); // Shape
        gen.swap();
        gen.push(i); // index
        Method maybeIntern =
            new Method(
                "maybeIntern",
                Type.getMethodDescriptor(
                    Type.getType(Object.class), Type.getType(Object.class), Type.INT_TYPE));
        gen.invokeVirtual(Type.getType(Shape.class), maybeIntern);
      }
      gen.putField(Type.getObjectType(clsName), "_" + i, types[i]);
    }
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateStreamCtor(
      ClassWriter cw, String clsName, Type[] types, Boolean asSeq) {
    String ctorDesc;
    if (asSeq)
      // SlottedBufferAsSeq doesn't need the Shape instance
      ctorDesc = Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(ObjectInput.class));
    else
      ctorDesc =
          Type.getMethodDescriptor(
              Type.VOID_TYPE,
              Type.getType(ObjectInput.class), // values
              Type.getType(Shape.class) // Shape
              );
    var mv = cw.visitMethod(ACC_PUBLIC, "<init>", ctorDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, "<init>", ctorDesc);
    gen.visitCode();
    gen.loadThis();
    if (asSeq) {
      gen.invokeConstructor(
          Type.getObjectType(SEQ_BASE_CLASS_NAME),
          new Method("<init>", Type.getMethodDescriptor(Type.VOID_TYPE)));
    } else {
      gen.loadArg(1); // Shape
      gen.invokeConstructor(
          Type.getObjectType(MAP_BASE_CLASS_NAME),
          new Method(
              "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Shape.class))));
    }
    var objectInputType = Type.getType(ObjectInput.class);
    for (int i = 0; i < types.length; ++i) {
      gen.loadThis();
      gen.loadArg(0); // in
      Method method = PRIMITIVE_STREAM_ACCESSOR_BY_TYPE.get(types[i]);
      if (null != method)
        // [SEE_FIELDPROTO_FOR_PRIMITIVES]
        // Field is a primitive type. Invoke the specific accessor to avoid boxing.
        gen.invokeInterface(objectInputType, method);
      else {
        // Field is Object type
        gen.invokeInterface(
            objectInputType, new Method("readObject", Type.getMethodDescriptor(types[i])));
        if (!asSeq) {
          // Field is Object type and this is a SlottedBufferAsMap. We want to go through
          // Shape.maybeIntern
          gen.loadArg(1); // Shape
          gen.swap();
          gen.push(i); // index
          Method maybeIntern =
              new Method(
                  "maybeIntern",
                  Type.getMethodDescriptor(
                      Type.getType(Object.class), Type.getType(Object.class), Type.INT_TYPE));
          gen.invokeVirtual(Type.getType(Shape.class), maybeIntern);
        }
      }
      gen.putField(Type.getObjectType(clsName), "_" + i, types[i]);
    }
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateStreamWrite(ClassWriter cw, String clsName, Type[] types) {
    String getDesc = Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(ObjectOutput.class));
    var mv = cw.visitMethod(ACC_PUBLIC, WRITE_METHOD_NAME, getDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, WRITE_METHOD_NAME, getDesc);
    gen.visitCode();
    var objectOutputType = Type.getType(ObjectOutput.class);
    for (int i = 0; i < types.length; ++i) {
      gen.loadArg(0); // out
      gen.loadThis();
      gen.getField(Type.getObjectType(clsName), "_" + i, types[i]);
      Method method = PRIMITIVE_STREAM_MUTATOR_BY_TYPE.get(types[i]);
      if (null == method) {
        method =
            new Method(
                "writeObject",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Object.class)));
      }
      gen.invokeInterface(objectOutputType, method);
    }
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateGet(ClassWriter cw, String clsName, Type[] types) {
    String getDesc = Type.getMethodDescriptor(Type.getType(Object.class), Type.INT_TYPE);
    var mv = cw.visitMethod(ACC_PUBLIC, GET_METHOD_NAME, getDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, GET_METHOD_NAME, getDesc);
    gen.visitCode();
    var keys = new int[types.length];
    for (int i = 0; i < keys.length; ++i) {
      keys[i] = i;
    }
    gen.loadArg(0);
    gen.tableSwitch(
        keys,
        new TableSwitchGenerator() {
          @Override
          public void generateCase(int key, Label end) {
            gen.loadThis();
            gen.getField(Type.getObjectType(clsName), "_" + key, types[key]);
            // valueOf ignores object types so we don't have to check for it here.
            gen.valueOf(types[key]);
            gen.goTo(end);
          }

          @Override
          public void generateDefault() {
            gen.visitFieldInsn(
                GETSTATIC,
                SB_CLASS_GENERATOR_CLASS,
                ILLEGAL_INDEX_NAME,
                Type.getDescriptor(Object.class));
          }
        });
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateLength(ClassWriter cw, int length) {
    var getDesc = Type.getMethodDescriptor(Type.INT_TYPE);
    var mv = cw.visitMethod(ACC_PUBLIC, "length", getDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, "length", getDesc);
    gen.visitCode();
    gen.push(length);
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateSignature(ClassWriter cw, String signature) {
    var getDesc = Type.getMethodDescriptor(Type.getType(String.class));
    var mv = cw.visitMethod(ACC_PUBLIC, "signature", getDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PUBLIC, "signature", getDesc);
    gen.visitCode();
    gen.push(signature);
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateArgsHash(ClassWriter cw, String clsName, Type[] types) {
    var hashCodeDesc = Type.getMethodDescriptor(Type.INT_TYPE);
    var mv = cw.visitMethod(ACC_PROTECTED, "argsHash", hashCodeDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PROTECTED, "argsHash", hashCodeDesc);
    gen.visitCode();
    gen.push(31);
    for (int i = 0; i < types.length; ++i) {
      gen.loadThis();
      gen.getField(Type.getObjectType(clsName), "_" + i, types[i]);
      if (types[i].getSort() != Type.OBJECT) {
        gen.invokeStatic(
            Type.getType(PluginHelpers.class),
            new Method("hashOf", Type.getMethodDescriptor(Type.INT_TYPE, Type.INT_TYPE, types[i])));
      } else {
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            Type.getType(InternerHashEquals.class).getInternalName(),
            "hashOf",
            Type.getMethodDescriptor(Type.INT_TYPE, Type.INT_TYPE, Type.getType(Object.class)),
            true /* is static interface method */);
      }
    }
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }

  private static void generateArgsEqual(ClassWriter cw, String clsName, Type[] types) {
    var equalsDesc = Type.getMethodDescriptor(Type.BOOLEAN_TYPE, Type.getType(Object.class));
    var mv = cw.visitMethod(ACC_PROTECTED, "argsEqual", equalsDesc, null, null);
    GeneratorAdapter gen = new GeneratorAdapter(mv, ACC_PROTECTED, "argsEqual", equalsDesc);
    gen.visitCode();
    var diff = new Label();
    // Generate:
    //   SB sb = (SB) o;
    gen.loadArg(0);
    // note that only internerEquals should be calling argsEqual, and that has already checked
    // classes match, so this cast should in practice always be safe
    gen.checkCast(Type.getObjectType(clsName));
    var castedLocal = gen.newLocal(Type.getObjectType(clsName));
    gen.storeLocal(castedLocal); // sb
    for (int i = 0; i < types.length; ++i) {
      gen.loadThis();
      gen.getField(Type.getObjectType(clsName), "_" + i, types[i]);
      gen.loadLocal(castedLocal);
      gen.getField(Type.getObjectType(clsName), "_" + i, types[i]);
      if (types[i].getSort() != Type.OBJECT) {
        gen.ifCmp(types[i], GeneratorAdapter.NE, diff); // If != 0 goto diff
      } else {
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            Type.getType(InternerHashEquals.class).getInternalName(),
            "equals",
            Type.getMethodDescriptor(
                Type.BOOLEAN_TYPE, Type.getType(Object.class), Type.getType(Object.class)),
            true /* is static interface method */);
        gen.ifZCmp(GeneratorAdapter.EQ, diff); // If == 0 goto diff
      }
    }
    // All match, return true
    gen.push(true);
    gen.returnValue();
    // diff, return false
    gen.visitLabel(diff);
    gen.push(false);
    gen.returnValue();
    gen.visitMaxs(0, 0); // passing zeros since we asked for COMPUTE_FRAMES
    gen.visitEnd();
  }
}
