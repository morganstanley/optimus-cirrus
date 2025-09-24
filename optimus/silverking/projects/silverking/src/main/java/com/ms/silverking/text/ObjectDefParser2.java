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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectDefParser2 {
  private static final ConcurrentMap<Class, ClassParser> classParserMap = new ConcurrentHashMap<>();
  public static final ObjectMapper objectMapper =
      YAMLMapper.builder()
          .enable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
          .activateDefaultTyping(
              new LaissezFaireSubTypeValidator(),
              ObjectMapper.DefaultTyping.NON_FINAL,
              JsonTypeInfo.As.PROPERTY)
          .build();

  private static Logger log = LoggerFactory.getLogger(ObjectDefParser2.class);

  static final boolean debug = false;
  private static final boolean debugAddParser = false;

  public static <T> void addParser(
      Class<T> _class,
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields,
      Class[] constructorFieldClasses,
      String[] constructorFieldNames) {
    if (debugAddParser) {
      log.info("addParser({})", _class.getName());
    }
    classParserMap.put(
        _class,
        new ClassParser<>(
            _class,
            template,
            fieldsRequirement,
            nonFatalExceptionResponse,
            fieldDefDelimiter,
            nameValueDelimiter,
            optionalFields,
            exclusionFields,
            constructorFieldClasses,
            constructorFieldNames));
  }

  public static <T> void addParser(
      Class<T> _class,
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    addParser(
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

  public static <T> void addParser(
      T template,
      FieldsRequirement fieldsRequirement,
      NonFatalExceptionResponse nonFatalExceptionResponse,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    addParser(
        (Class<T>) template.getClass(),
        template,
        fieldsRequirement,
        nonFatalExceptionResponse,
        fieldDefDelimiter,
        nameValueDelimiter,
        optionalFields,
        exclusionFields);
  }

  public static <T> void addParser(
      T template,
      FieldsRequirement fieldsRequirement,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    addParser(
        template,
        fieldsRequirement,
        NonFatalExceptionResponse.THROW_EXCEPTIONS,
        fieldDefDelimiter,
        nameValueDelimiter,
        optionalFields,
        exclusionFields);
  }

  public static <T> void addParser(
      Class<T> _class,
      T template,
      FieldsRequirement fieldsRequirement,
      String fieldDefDelimiter,
      String nameValueDelimiter,
      Set<String> optionalFields,
      Set<String> exclusionFields) {
    addParser(
        _class,
        template,
        fieldsRequirement,
        NonFatalExceptionResponse.THROW_EXCEPTIONS,
        fieldDefDelimiter,
        nameValueDelimiter,
        optionalFields,
        exclusionFields);
  }

  public static <T> void addParser(
      T template, FieldsRequirement fieldsRequirement, Set<String> optionalFields) {
    addParser(template, fieldsRequirement, null, null, optionalFields, null);
  }

  public static <T> void addParser(
      T template,
      FieldsRequirement fieldsRequirement,
      Set<String> optionalFields,
      Class[] constructorFieldClasses,
      String[] constructorFieldNames) {
    addParser(
        (Class<T>) template.getClass(),
        template,
        fieldsRequirement,
        NonFatalExceptionResponse.THROW_EXCEPTIONS,
        null,
        null,
        optionalFields,
        null,
        constructorFieldClasses,
        constructorFieldNames);
  }

  public static <T> void addParser(
      T template, Set<String> optionalFields, Set<String> exclusionFields) {
    addParser(
        template, FieldsRequirement.ALLOW_INCOMPLETE, null, null, optionalFields, exclusionFields);
  }

  public static <T> void addParser(T template) {
    addParser(template, FieldsRequirement.ALLOW_INCOMPLETE, null, null, null, null);
  }

  public static <T> void addParser(T template, FieldsRequirement fieldsRequirement) {
    addParser(template, fieldsRequirement, null, null, null, null);
  }

  public static <T> void addParserWithExclusions(T template, Set<String> exclusionFields) {
    addParser(template, FieldsRequirement.ALLOW_INCOMPLETE, null, null, null, exclusionFields);
  }

  public static <T> void addParserWithExclusions(
      Class<T> _class,
      T template,
      FieldsRequirement fieldsRequirement,
      Set<String> exclusionFields) {
    addParser(_class, template, fieldsRequirement, null, null, null, exclusionFields);
  }

  public static <T> void addParserWithOptionalFields(T template, Set<String> optionalFields) {
    addParser(template, FieldsRequirement.ALLOW_INCOMPLETE, null, null, optionalFields, null);
  }

  public static <T> void addParser(Class<T> _class, T template) {
    addParser(_class, template, FieldsRequirement.ALLOW_INCOMPLETE, null, null, null, null);
  }

  public static String toClassAndDefString(Object o) {
    return "<" + o.getClass().getCanonicalName() + ">{" + ObjectDefParser2.objectToString(o) + "}";
  }

  public static <T> T parse(String nameAndDef, Package defaultPackage) {
    Class<T> _class;
    String def;

    nameAndDef = nameAndDef.trim();
    if (nameAndDef.startsWith("<")) {
      int i1;
      String classDef;

      i1 = nameAndDef.indexOf('>');
      if (i1 < 0) {
        throw new RuntimeException("Bad nameAndDef. Missing >");
      } else {
        classDef = nameAndDef.substring(1, i1);
        def = nameAndDef.substring(i1 + 1);
        if (classDef.indexOf('.') < 0 && defaultPackage != null) {
          classDef = defaultPackage.getName() + "." + classDef;
        }
      }
      try {
        _class = (Class<T>) Class.forName(classDef);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Can't find class for: " + classDef);
      }
    } else {
      throw new RuntimeException("Bad nameAndDef. Missing <");
    }

    return parse(_class, def);
  }

  public static <T> T parse(Class<T> clazz, String def) {
    if (def.startsWith("---")) {
      return parseFromYaml(clazz, def);
    } else {
      return parseFromObjDefFormat(clazz, def);
    }
  }

  private static <T> T parseFromYaml(Class<T> clazz, String def) {
    try {
      return objectMapper.readValue(def, clazz);
    } catch (JsonProcessingException e) {
      throw new ObjectDefParseException("Could not parse value of type " + clazz.getName(), e);
    }
  }

  private static <T> T parseFromObjDefFormat(Class<T> _class, String def) {
    ClassParser<T> cp;
    Class<T> type;

    if (debug) {
      Thread.dumpStack();
      log.debug("odp2.parse: {}", _class.getName());
    }
    type = _class;
    if (def.startsWith(Character.toString(ClassParser.typeNameDelimiterStart))) {
      int typeNameEnd;

      typeNameEnd = def.indexOf(ClassParser.typeNameDelimiterEnd);
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
          type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException cnfe) {
          throw new ObjectDefParseException(cnfe);
        }
      }

      if (def.startsWith(Character.toString(ClassParser.recursiveDefDelimiterStart))
          && def.endsWith(Character.toString(ClassParser.recursiveDefDelimiterEnd))) {
        def = def.substring(1, def.length() - 1);
      } else {
        throw new ObjectDefParseException("sub def not delimited: " + def);
      }
    }

    cp = getClassParser(type);
    return cp.parse(def);
  }

  public static <T> T parse(Class<T> _class, T template, String def) {
    ClassParser<T> cp;

    cp =
        new ClassParser<T>(
            template,
            FieldsRequirement.ALLOW_INCOMPLETE,
            NonFatalExceptionResponse.THROW_EXCEPTIONS,
            null,
            null);
    return cp.parse(def);
  }

  public static <T> String objectToString(T obj) {
    return objectToString((Class<T>) obj.getClass(), obj);
  }

  public static <T> String objectToStringWithExclusions(
      T obj, Set<String> overrideExclusionFields) {
    return objectToString((Class<T>) obj.getClass(), obj, overrideExclusionFields);
  }

  public static <T> String objectToString(Class<T> _class, T obj) {
    return objectToString(_class, obj, null);
  }

  public static <T> String objectToString(
      Class<T> _class, T obj, Set<String> overrideExclusionFields) {
    if (_class.equals(String.class)) {
      return (String) obj;
    } else {
      ClassParser<T> cp;

      if (debug) {
        log.debug("objectToString {}", _class.getName());
      }
      cp = getClassParser(_class);
      return cp.objectToString(obj, overrideExclusionFields);
    }
  }

  public static <T> boolean isKnownType(Class<T> _class) {
    return classParserMap.get(_class) != null;
  }

  private static <T> ClassParser<T> getClassParser(Class<T> _class) {
    ClassParser<T> cp;

    cp = classParserMap.get(_class);
    if (cp == null) {
      throw new ObjectDefParseException("Can't find ClassParser for " + _class);
    } else {
      return cp;
    }
  }

  private static class ClassAndFieldName {
    final Class _class;
    final String fieldName;

    ClassAndFieldName(Class _class, String fieldName) {
      this._class = _class;
      this.fieldName = fieldName;
    }

    @Override
    public int hashCode() {
      return _class.hashCode() ^ fieldName.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      ClassAndFieldName o;

      o = (ClassAndFieldName) other;
      return _class.equals(o._class) && fieldName.equals(o.fieldName);
    }

    @Override
    public String toString() {
      return _class.getName() + "." + fieldName;
    }
  }

  private static Map<ClassAndFieldName, Class> setTypes = new HashMap<>();

  public static void addSetType(Class _class, String fieldName, Class type) {
    // System.out.println("addSetType: "+ new ClassAndFieldName(_class, fieldName) +"\t"+ type);
    setTypes.put(new ClassAndFieldName(_class, fieldName), type);
  }

  private static Class getSetType(Class _class, String fieldName) {
    // System.out.println("getSetType: "+ new ClassAndFieldName(_class, fieldName)
    //    +"\t"+ setTypes.get(new ClassAndFieldName(_class, fieldName)));
    return setTypes.get(new ClassAndFieldName(_class, fieldName));
  }

  public static Set parseSet(Set<String> defs, Class _class, String fieldName) {
    Class setType;

    setType = getSetType(_class, fieldName);
    if (setType == null) {
      return defs;
    } else {
      return parseSet(defs, setType);
    }
  }

  public static Set parseSet(String defs, Class setType) {
    return parseSet(ClassParser.parseSet(defs), setType);
  }

  public static Set parseSet(Set<String> defs, Class setType) {
    ImmutableSet.Builder set;

    set = ImmutableSet.builder();
    for (String def : defs) {
      set.add(parse(setType, def));
    }
    return set.build();
  }
}
