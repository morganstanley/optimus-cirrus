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
package com.ms.silverking.text;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.util.ArrayUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class ClassParser<T> {
  private final T template;
  private final Class<T> _class;
  private final Constructor<T> constructor;
  private final Field[] fields;
  private final FieldsRequirement fieldRequirement;
  private final NonFatalExceptionResponse nonFatalExceptionResponse;
  private final String fieldDefDelimiter;
  private final String nameValueDelimiter;
  private final Set<String> optionalFields;
  private final Set<String> exclusionFields;
  private final Class[] constructorFieldClasses;
  private final String[] constructorFieldNames;

  private static Logger log = LoggerFactory.getLogger(ClassParser.class);

  private static final char defaultFieldDefDelimiter = ',';
  private static final char defaultNameValueDelimiter = '=';
  static final char recursiveDefDelimiterStart = '{';
  static final char recursiveDefDelimiterEnd = '}';
  static final char typeNameDelimiterStart = '<';
  static final char typeNameDelimiterEnd = '>';

  ClassParser(
      Class _class,
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields,
      Class[] constructorFieldClasses,
      String[] constructorFieldNames) {
    Field[] _fields;

    if ((constructorFieldClasses == null) != (constructorFieldNames == null)) {
      throw new RuntimeException(
          "(constructorFieldClasses == null) != (constructorFieldNames == null)");
    }
    if (constructorFieldClasses != null
        && (constructorFieldClasses.length != constructorFieldNames.length)) {
      throw new RuntimeException("constructorFieldClasses.length != constructorFieldNames.length");
    }
    this.constructorFieldClasses = constructorFieldClasses;
    this.constructorFieldNames = constructorFieldNames;
    _fields = null;
    try {
      this.template = template;
      this.fieldRequirement = fieldsRequirement;
      this.nonFatalExceptionResponse = nonFatalExceptionResponse;
      if (fieldDefDelimiter != null) {
        this.fieldDefDelimiter = fieldDefDelimiter;
      } else {
        this.fieldDefDelimiter = Character.toString(defaultFieldDefDelimiter);
      }
      if (nameValueDelimiter != null) {
        this.nameValueDelimiter = nameValueDelimiter;
      } else {
        this.nameValueDelimiter = Character.toString(defaultNameValueDelimiter);
      }
      if (optionalFields != null) {
        if (fieldsRequirement == FieldsRequirement.REQUIRE_ALL_FIELDS) {
          throw new RuntimeException(
              "Optional fields incompatible with FieldsRequirement.REQUIRE_ALL_FIELDS");
        }
        this.optionalFields = optionalFields;
      } else {
        this.optionalFields = ImmutableSet.of();
      }
      if (exclusionFields != null) {
        this.exclusionFields = exclusionFields;
      } else {
        this.exclusionFields = ImmutableSet.of();
      }
      this._class = _class;
      // fields = filterStaticFields(_class.getDeclaredFields());
      fields =
          CPUtils.filterFields(
              CPUtils.filterStaticFields(CPUtils.getDeclaredFields(_class)), this.exclusionFields);
      // constructor = _class.getConstructor(getFieldClasses(fields));
      _fields = fields;
      if (Modifier.isAbstract(_class.getModifiers())) {
        constructor = null;
      } else {
        if (constructorFieldClasses == null) {
          constructorFieldClasses = CPUtils.getFieldClasses(fields);
        }
        constructor = CPUtils.getConstructor(_class, constructorFieldClasses);
      }
    } catch (NoSuchMethodException nsme) {
      if (_fields != null) {
        log.info("************************************");
        log.info("***    Can't find constructor    ***");
        log.info("************************************");
        log.info(ArrayUtil.toString(CPUtils.getFieldClasses(_fields)));
      }
      throw new ObjectDefParseException(
          "Can't find constructor to initialize all fields for "
              + _class.getName()
              + ". "
              + "Remember order must "
              + "match",
          nsme);
    } catch (Exception e) {
      throw new ObjectDefParseException("Error creating template " + _class.getName(), e);
    }
  }

  ClassParser(
      Class _class,
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    this(
        _class,
        template,
        fieldsRequirement,
        nonFatalExceptionResponse,
        fieldDefDelimiter,
        nameValueDelimiter,
        optionalFields,
        exclusionFields,
        null,
        null);
  }

  ClassParser(
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    this(
        template.getClass(),
        template,
        fieldsRequirement,
        nonFatalExceptionResponse,
        fieldDefDelimiter,
        nameValueDelimiter,
        optionalFields,
        exclusionFields);
  }

  ClassParser(
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    this(
        template,
        fieldsRequirement,
        nonFatalExceptionResponse,
        Character.toString(defaultFieldDefDelimiter),
        Character.toString(defaultNameValueDelimiter),
        optionalFields,
        exclusionFields);
  }

  private Object[] createConstructorArgsFromParams(Map<String, String> defMap) {
    Object[] args;
    Parameter[] cParams;

    cParams = constructor.getParameters();
    args = new Object[cParams.length];
    for (int i = 0; i < cParams.length; i++) {
      String name;
      String value;

      name = constructorFieldNames[i];
      value = defMap.get(name);
      if (value == null) {
        throw new ObjectDefParseException("Missing parameter: " + name + " in " + _class.getName());
      } else {
        try {
          args[i] = valueForDef(cParams[i].getType(), value, name);
        } catch (ObjectDefParseException e) {
          if (nonFatalExceptionResponse == NonFatalExceptionResponse.LOG_EXCEPTIONS) {
            log.error("Logging and ignoring", e);
          }
          if (nonFatalExceptionResponse != NonFatalExceptionResponse.THROW_EXCEPTIONS) {
            try {
              fields[i].setAccessible(true);
              args[i] = fields[i].get(template);
            } catch (Exception e2) {
              throw new ObjectDefParseException(
                  "Unable to set field value from template in " + _class.getName(), e2);
            }
          } else {
            throw e;
          }
        }
      }
    }
    return args;
  }

  private Object[] createConstructorArgs(Map<String, String> defMap) {
    Object[] args;
    args = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String name;
      String value;

      name = fields[i].getName();
      value = defMap.get(name);
      if (value == null) {
        if (fieldRequirement == FieldsRequirement.REQUIRE_ALL_FIELDS) {
          throw new ObjectDefParseException(
              "Missing required field: " + name + " in " + _class.getName());
        } else if (fieldRequirement == FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS
            && !optionalFields.contains(name)) {
          throw new ObjectDefParseException(
              "Missing required field: " + name + " in " + _class.getName());
        } else {
          try {
            fields[i].setAccessible(true);
            args[i] = fields[i].get(template);
          } catch (Exception e) {
            throw new ObjectDefParseException(
                "Unable to set field value from template in " + _class.getName(), e);
          }
        }
      } else {
        try {
          args[i] = valueForDef(fields[i], value);
        } catch (ObjectDefParseException e) {
          if (nonFatalExceptionResponse == NonFatalExceptionResponse.LOG_EXCEPTIONS) {
            log.error("Logging and ignoring", e);
          }
          if (nonFatalExceptionResponse != NonFatalExceptionResponse.THROW_EXCEPTIONS) {
            try {
              fields[i].setAccessible(true);
              args[i] = fields[i].get(template);
            } catch (Exception e2) {
              throw new ObjectDefParseException(
                  "Unable to set field value from template in " + _class.getName(), e2);
            }
          } else {
            throw e;
          }
        }
      }
    }
    return args;
  }

  private Object valueForDef(Field field, String def) {
    return valueForDef(field.getType(), def, field.getName());
  }

  private Object valueForDef(Class type, String def, String fieldName) {
    if (type.isEnum()) {
      try {
        Method valueOf;

        valueOf = type.getMethod("valueOf", String.class);
        return valueOf.invoke(null, def);
      } catch (Exception e) {
        throw new ObjectDefParseException("Unable to create enum", e);
      }
    } else {
      // FUTURE - consider invoking a generic string-supporting constructor
      if (type == Byte.class || type == byte.class) {
        return Byte.parseByte(def);
      } else if (type == Character.class || type == char.class) {
        return new Character(def.charAt(0));
      } else if (type == Short.class || type == short.class) {
        return Short.parseShort(def);
      } else if (type == Integer.class || type == int.class) {
        return Integer.parseInt(def);
      } else if (type == Long.class || type == long.class) {
        return Long.parseLong(def);
      } else if (type == Float.class || type == float.class) {
        return Float.parseFloat(def);
      } else if (type == Double.class || type == double.class) {
        return Double.parseDouble(def);
      } else if (type == String.class) {
        return def;
      } else if (type == Boolean.class || type == boolean.class) {
        return Boolean.parseBoolean(def);
      } else if (type == Map.class) {
        return parseMap(def);
      } else if (type == Set.class) {
        return ObjectDefParser2.parseSet(parseSet(def), _class, fieldName);
      } else {
        if (def.startsWith(Character.toString(typeNameDelimiterStart))) {
          int typeNameEnd;

          typeNameEnd = def.indexOf(typeNameDelimiterEnd);
          if (typeNameEnd < 0) {
            log.info("type: {}", type);
            log.info("def: {}", def);
            throw new ObjectDefParseException("\n" + type + " Missing typeNameDelimiterEnd " + def);
          } else if (typeNameEnd >= def.length() - 1) {
            log.info("type: {}", type);
            log.info("def: {}", def);
            throw new ObjectDefParseException("\n" + type + " Found type, missing def " + def);
          } else {
            String typeName;

            typeName = def.substring(1, typeNameEnd);
            def = def.substring(typeNameEnd + 1);
            if (typeName.indexOf('.') < 0) {
              typeName = type.getPackage().getName() + "." + typeName;
            }
            try {
              type = Class.forName(typeName);
            } catch (ClassNotFoundException cnfe) {
              throw new ObjectDefParseException(cnfe);
            }
          }
        }

        if (def.startsWith(Character.toString(recursiveDefDelimiterStart))
            && def.endsWith(Character.toString(recursiveDefDelimiterEnd))) {
          Object value;

          def = def.substring(1, def.length() - 1);
          // System.out.println("\n\n"+ def);
          // value = new ObjectDefParser(subTemplateAndOptions.getObj(), fieldRequirement,
          // nonFatalExceptionResponse,
          //            def, def, subTemplateAndOptions.getOptionalFields()).parse(def);

          /*
          Method  parseMethod;

          try {
              parseMethod = subTemplateAndOptions.getObj().getClass().getMethod("parse", String.class);
              value = parseMethod.invoke(subTemplateAndOptions, def);
          } catch (NoSuchMethodException e) {
              throw new RuntimeException(e);
          } catch (SecurityException e) {
              throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
              throw new RuntimeException(e);
          } catch (IllegalArgumentException e) {
              throw new RuntimeException(e);
          } catch (InvocationTargetException e) {
              throw new RuntimeException(e);
          }
          */
          value = ObjectDefParser2.parse(type, def);

          return value;
        } else {
          throw new ObjectDefParseException("sub def not delimited: " + def);
        }
      }
    }
  }

  public T parse(String fieldDefs) {
    Map<String, String> defMap;

    fieldDefs = stripExtraDelimiters(fieldDefs);
    defMap = new HashMap<>();
    for (String fieldDef : splitFieldDefs(fieldDefs)) {
      fieldDef = fieldDef.trim();
      if (fieldDef.length() > 0) {
        String[] nameAndValue;
        String fieldName;
        String valueDef;

        nameAndValue = splitNameAndValue(fieldDef);
        if (nameAndValue.length != 2) {
          throw new ObjectDefParseException("bad nameAndValue: " + fieldDef);
        }
        fieldName = nameAndValue[0];
        valueDef = nameAndValue[1];
        defMap.put(fieldName, valueDef);
      } else {
        log.info("Ignoring empty field def in {}", _class);
      }
    }
    try {
      Object[] constructorArgs;
      T newInstance;

      try {
        constructorArgs = createConstructorArgs(defMap);
      } catch (ObjectDefParseException odpe) {
        constructorArgs = createConstructorArgsFromParams(defMap);
      }
      newInstance = constructor.newInstance(constructorArgs);
      return newInstance;
    } catch (Exception e) {
      throw new ObjectDefParseException("Exception creating instance", e);
    }
  }

  private String stripExtraDelimiters(String def) {
    while (def.length() > 0) {
      def = def.trim();
      if (def.charAt(0) == recursiveDefDelimiterStart
          && def.charAt(def.length() - 1) == recursiveDefDelimiterEnd) {
        def = def.substring(1, def.length() - 1);
      } else {
        return def;
      }
    }
    return def;
  }

  private String[] splitNameAndValue(String fieldDef) {
    int i;

    i = fieldDef.indexOf(defaultNameValueDelimiter);
    if (i < 0) {
      return new String[0];
    } else {
      String[] s;

      s = new String[2];
      s[0] = fieldDef.substring(0, i);
      s[1] = fieldDef.substring(i + 1);
      return s;
    }
  }

  private String[] splitFieldDefs(String fieldDefs) {
    // return fieldDefs.split(fieldDefDelimiter);
    List<String> defs;
    int depth;
    int last;

    last = 0;
    depth = 0;
    defs = new ArrayList<>();
    for (int i = 0; i < fieldDefs.length(); i++) {
      switch (fieldDefs.charAt(i)) {
        case defaultFieldDefDelimiter:
          if (depth == 0) {
            defs.add(fieldDefs.substring(last, i));
            last = i + 1;
          }
          break;
        case recursiveDefDelimiterStart:
          depth++;
          break;
        case recursiveDefDelimiterEnd:
          depth--;
          break;
      }
    }
    if (last < fieldDefs.length()) {
      defs.add(fieldDefs.substring(last, fieldDefs.length()));
    }
    return defs.toArray(new String[0]);
  }

  public String objectToString(T obj) {
    return objectToString(obj, null);
  }

  public String objectToString(T obj, Set<String> overrideExclusionFields) {
    Class superClass;
    String superClassString;
    StringBuilder sb;
    Field[] fields;

    sb = new StringBuilder();
    superClass = _class.getSuperclass();
    if (superClass != Object.class) {
      superClassString = ObjectDefParser2.objectToString(superClass, obj);
      sb.append(superClassString);
      if (!superClassString.endsWith(fieldDefDelimiter)) {
        sb.append(fieldDefDelimiter);
      }
    }

    if (overrideExclusionFields == null) {
      overrideExclusionFields = exclusionFields;
    }
    fields =
        CPUtils.filterFields(
            CPUtils.filterStaticFields(_class.getDeclaredFields()), overrideExclusionFields);
    for (int i = 0; i < fields.length; i++) {
      Field field;
      boolean recursive;
      String valueString;
      String typeNameString;

      valueString = null;
      typeNameString = null;
      field = fields[i];
      if (field.getType() == Set.class) {
        field.setAccessible(true);
        try {
          if (field.get(obj) == null) {
            // valueString = "<error null>";
            valueString = null;
          } else {
            // valueString = CollectionUtil.toString((Set)field.get(obj), ',');
            valueString = setToString((Set) field.get(obj));
          }
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        recursive = false;
      } else {
        if (field.getType().isInterface() && !isSpecialCase(field.getType())) {
          String ns;
          Class actualFieldType;

          recursive = true;
          try {
            Object fObj;

            field.setAccessible(true);
            fObj = field.get(obj);
            if (fObj != null) {
              actualFieldType = fObj.getClass();
            } else {
              actualFieldType = null;
            }
          } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
          if (actualFieldType != null) {
            if (obj.getClass().getPackage() == _class.getPackage()) {
              ns = actualFieldType.getSimpleName();
            } else {
              ns = actualFieldType.getName();
            }
            typeNameString = typeNameDelimiterStart + ns + typeNameDelimiterEnd;
          } else {
            typeNameString = null;
          }
        } else {
          recursive = ObjectDefParser2.isKnownType(field.getType());
        }

        try {
          Object value;

          value = field.get(obj);
          // if (value != null || !optionalFields.contains(field.getName())) {
          if (value != null) {
            valueString = value.toString();
          }
        } catch (IllegalAccessException e) {
          try {
            Object value;

            field.setAccessible(true);
            value = field.get(obj);
            if (value != null) {
              valueString = value.toString();
            }
          } catch (Exception e2) {
            throw new RuntimeException(e2);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      if (valueString != null) {
        sb.append(field.getName());
        sb.append(nameValueDelimiter);
        if (recursive) {
          if (typeNameString != null) {
            sb.append(typeNameString);
          }
          sb.append(recursiveDefDelimiterStart);
        }
        sb.append(valueString);
        if (recursive) {
          sb.append(recursiveDefDelimiterEnd);
        }
        if (i < fields.length - 1) {
          sb.append(fieldDefDelimiter);
        }
      }
    }
    return sb.toString();
  }

  private String setToString(Set set) {
    if (set.size() == 0) {
      return "{}";
    } else {
      StringBuilder sb;

      sb = new StringBuilder();
      sb.append('{');
      for (Object o : set) {
        sb.append('{');
        sb.append(ObjectDefParser2.objectToString(o));
        sb.append('}');
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append('}');
      return sb.toString();
    }
  }

  private boolean isSpecialCase(Class<?> type) {
    return type == Map.class || type == Set.class;
  }

  private Map<String, String> parseMap(String def) {
    try {
      def = def.trim();
      if (!def.startsWith("" + recursiveDefDelimiterStart)) {
        throw new RuntimeException("Bad map def: " + def);
      }
      if (!def.endsWith("" + recursiveDefDelimiterEnd)) {
        throw new RuntimeException("Bad map def: " + def);
      }
      def = def.substring(1, def.length() - 1);
      if (def.indexOf(',') >= 0) {
        def = def.replace(',', '\n');
      }
      return StreamParser.parseMap(new ByteArrayInputStream(def.getBytes()));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  static Set<String> parseSet(String def) {
    boolean delimitedEntries;
    String originalDef;

    originalDef = def;
    // try {
    def = def.trim();
    if (!def.startsWith("" + recursiveDefDelimiterStart)) {
      throw new RuntimeException("Bad map def: " + def);
    }
    if (!def.endsWith("" + recursiveDefDelimiterEnd)) {
      throw new RuntimeException("Bad map def: " + def);
    }
    def = def.substring(1, def.length() - 1).trim(); // remove delimiters
    delimitedEntries = def.startsWith("" + recursiveDefDelimiterStart);
    // return StreamParser.parseSet(new ByteArrayInputStream(def.getBytes()));
    ImmutableSet.Builder<String> builder;

    builder = ImmutableSet.builder();
    while (def.length() > 0) {
      int entryStart;
      int entryEnd;
      int nextEntryStart;
      String entry;

      if (delimitedEntries) {
        if (!def.startsWith("" + recursiveDefDelimiterStart)) {
          throw new RuntimeException("Missing " + recursiveDefDelimiterStart + ": " + originalDef);
        }
        entryStart = 1;
        entryEnd = def.indexOf(recursiveDefDelimiterEnd);
        if (entryEnd < 0) {
          throw new RuntimeException("Missing " + recursiveDefDelimiterEnd + ": " + originalDef);
        }
        nextEntryStart = def.indexOf(recursiveDefDelimiterStart, entryEnd);
      } else {
        entryStart = 0;
        entryEnd = def.indexOf(',');
        if (entryEnd < 0) {
          entryEnd = def.length();
          nextEntryStart = -1;
        } else if (entryEnd == def.length() - 1) {
          nextEntryStart = -1;
        } else {
          nextEntryStart = entryEnd + 1;
        }
      }
      // System.out.println(originalDef);
      // System.out.println(def);
      // System.out.println(entryStart +" "+ entryEnd);
      entry = def.substring(entryStart, entryEnd);
      builder.add(entry);
      if (nextEntryStart < 0) {
        def = "";
      } else {
        def = def.substring(nextEntryStart).trim();
      }
    }
    return builder.build();
    // } catch (IOException ioe) {
    //    throw new RuntimeException(ioe);
    // }
  }
}
