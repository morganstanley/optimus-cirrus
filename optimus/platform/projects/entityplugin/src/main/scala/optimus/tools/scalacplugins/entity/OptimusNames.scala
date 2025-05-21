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
package optimus.tools.scalacplugins.entity

import scala.tools.nsc.Global

trait OptimusNames {
  val global: Global
  import global._

  object names {
    // packages
    val annotations: TermName = newTermName("annotations")
    val core: TermName = newTermName("core")
    val graph: TermName = newTermName("graph")
    val dist: TermName = newTermName("dist")
    val dal: TermName = newTermName("dal")
    val info: TermName = newTermName("info")
    val internal: TermName = newTermName("internal")
    val optimus: TermName = newTermName("optimus")
    val pickling: TermName = newTermName("pickling")
    val scalacompat: TermName = newTermName("scalacompat")
    val collection: TermName = newTermName("collection")
    val platform: TermName = newTermName("platform")
    val storable: TermName = newTermName("storable")
    val util: TermName = newTermName("util")
    val reflect: TermName = newTermName("reflect")
    val versioning: TermName = newTermName("versioning")
    val asyncOff: TermName = newTermName("asyncOff")

    // modules
    val DAL: TermName = newTermName("DAL")
    val EvaluationContext: TermName = newTermName("EvaluationContext")
    val CoreAPI: TermName = newTermName("CoreAPI")
    val PluginSupport: TermName = newTermName("PluginSupport")
    val CoreSupport: TermName = newTermName("CoreSupport")
    val PluginHelpers: TermName = newTermName("PluginHelpers")
    val CanEqual: TermName = newTermName("CanEqual")
    val PluginMacros: TermName = newTermName("PluginMacros")
    val Tweaks: TermName = newTermName("Tweaks")
    val None: TermName = newTermName("None")
    val RuntimeConstants: TermName = newTermName("RuntimeConstants")

    // members
    val findByIndex: TermName = newTermName("findByIndex")
    val genIndexInfo: TermName = newTermName("genIndexInfo")
    val genQueryableIndexInfo: TermName = newTermName("genQueryableIndexInfo")
    val genQueryableIndexInfoWithErefFilter: TermName = newTermName("genQueryableIndexInfoWithErefFilter")
    val default: TermName = newTermName("default")
    val getOption: TermName = newTermName("getOption")
    val current: TermName = newTermName("current")
    val verifyOffGraph: TermName = newTermName("verifyOffGraph")
    val verifyImpure: TermName = newTermName("verifyImpure")
    val getJob: TermName = newTermName("getJob") // [JOB_EXPERIMENTAL] On @job-annotated Node
    val enqueue: TermName = newTermName("enqueue") // On Node
    val enqueueJob: TermName = newTermName("enqueueJob") // [JOB_EXPERIMENTAL] On @job-annotated Node
    val locally: TermName = newTermName("locally")
    val lookupAndEnqueue: TermName = newTermName("lookupAndEnqueue") // On PropertyNode
    val lookupAndEnqueueJob: TermName = newTermName(
      "lookupAndEnqueueJob"
    ) // [JOB_EXPERIMENTAL] On @job-annotated PropertyNode
    val lookupAndGet: TermName = newTermName("lookupAndGet") // On PropertyNode
    val lookupAndGetJob: TermName = newTermName(
      "lookupAndGetJob"
    ) // [JOB_EXPERIMENTAL] On @job-annotated PropertyNode
    val lookupAndGetSI: TermName = newTermName("lookupAndGetSI") // On PropertyNode
    val makeKey: TermName = newTermName("makeKey")
    val nodify: TermName = newTermName("nodify")
    val nodifyProperty: TermName = newTermName("nodifyProperty")
    val toNode: TermName = newTermName("toNode")
    val temporal: TermName = newTermName("temporal")
    val temporalContext: TermName = newTermName("temporalContext")
    val scenarioStack: TermName = newTermName("scenarioStack")
    val $info: TermName = newTermName("$info")
    val propertyInfo: TermName = newTermName("propertyInfo")
    val executionInfo: TermName = newTermName("executionInfo")
    val tweakHandlerSuffix: TermName = newTermName("_$colon$eq") // scala.reflect.NameTransformer.encode("_:=")
    val args: TermName = newTermName("args")
    val argsEquals: TermName = newTermName("argsEquals")
    val argsHash: TermName = newTermName("argsHash")
    val argsCopy: TermName = newTermName("argsCopy")
    val $4eq: TermName = newTermName("$4eq")
    val entity: TermName = newTermName("entity")
    val unique: TermName = newTermName("unique")
    val queryable: TermName = newTermName("queryable")
    val queryByEref: TermName = newTermName("queryByEref")
    val ensureNotCacheable: TermName = newTermName("ensureNotCacheable")
    val result: TermName = newTermName("result")
    val childNode: TermName = newTermName("childNode")
    val getLinkedEntities: TermName = newTermName("getLinkedEntities")
    val now: TermName = newTermName("now")
    val contained: TermName = newTermName("contained")
    val KeyInfo: TermName = newTermName(suffixes.KEY_INFO)

    val funcSync: TermName = newTermName("func")
    val funcFSM: TermName = newTermName("funcFSM")
    val findEntity: TermName = newTermName("findEntity")
    val findEntityOption: TermName = newTermName("findEntityOption")
    val findEvent: TermName = newTermName("findEvent")
    val findEventOption: TermName = newTermName("findEventOption")
    val hashOf: TermName = newTermName("hashOf")
    val equals: TermName = newTermName("equals")
    val equalsAvoidingZero: TermName = newTermName("equalsAvoidingZero")

    val canEqual: TermName = newTermName("canEqual") // overloaded for String, Enum, Object, GenTraversableOnce
    val canEqualStorable: TermName = newTermName(
      "canEqualStorable"
    ) // separate to avoid issues with entity extends traversable

    val matchOn_eq: TermName = newTermName("matchOn_$eq")
    val tweak: TermName = newTermName("tweak")
    val exposeWithArgTypes: TermName = newTermName("exposeArgTypes")
    val childToParent: TermName = newTermName("childToParent")
    val colonEquals: TermName = newTermName("$colon$eq")

    val transformTweak: TermName = newTermName("transformTweak")

    val resolver: TermName = newTermName("resolver")
    val entityResolver: TermName = newTermName("entityResolver")
    val dalEntityRef: TermName = newTermName("dal$entityRef")
    val dalLoadContext: TermName = newTermName("dal$loadContext")
    val dalTemporalContext: TermName = newTermName("dal$temporalContext")
    val seek: TermName = newTermName("seek")
    val seekRaw: TermName = newTermName("seekRaw")
    val pickler: TermName = newTermName("pickler")
    val unpickler: TermName = newTermName("unpickler")
    val write: TermName = newTermName("write")
    val writeEntity: TermName = newTermName("writeEntity")
    val writeFieldName: TermName = newTermName("writeFieldName")
    val writePropertyInfo: TermName = newTermName("writePropertyInfo")
    val writeStartObject: TermName = newTermName("writeStartObject")
    val writeEndObject: TermName = newTermName("writeEndObject")
    val given: TermName = TermName("given")

    val seq: TermName = newTermName("seq")
    val par: TermName = newTermName("par")
    val forall: TermName = newTermName("forall")
    val exists: TermName = newTermName("exists")
    val getOrElse: TermName = newTermName("getOrElse")
    val orElse: TermName = newTermName("orElse")
    val isDefined: TermName = newTermName("isDefined")
    val collect: TermName = newTermName("collect")
    val empty: TermName = newTermName("empty")

    val AppliedStorageInfo: TermName = newTermName("AppliedStorageInfo")
    val UniqueStorageInfo: TermName = newTermName("UniqueStorageInfo")
    val TemporalContext: TermName = newTermName("TemporalContext")
    val uniqueInstance: TermName = newTermName("uniqueInstance")
    val uniqueInstance$default: TermName = newTermName("uniqueInstance$default")

    val allowConstructorDefaultValuesInVersioning: TermName = newTermName("allowConstructorDefaultValuesInVersioning")

    val auto: TermName = newTermName("auto")
    val Async: TermName = newTermName("Async")
    val expect: TermName = newTermName("expect")

    val assert: TermName = newTermName("assert")
    val require: TermName = newTermName("require")

    val foldLeft: TermName = newTermName("foldLeft")
    val cachedHashcode: TermName = newTermName("optimus$cachedHashcode")
    val cachedHashcodeValid: TermName = newTermName("optimus$cachedHashcodeValid")

    // parameters of entity annotation
    val schemaVersion: TermName = newTermName("schemaVersion")
    val projected: TermName = newTermName("projected")
    val projectedMembers: TermName = newTermName("projectedMembers")
    val indexed: TermName = newTermName("indexed")
    val fullTextSearch: TermName = newTermName("fullTextSearch")
    val monoTemporal: TermName = newTermName("monoTemporal")
    // not really a name but associated with it
    val schemaVersionDefaultValue = 0
    val projectedDefaultValue = false
    val containedDefaultValue = false
    val fullTextSearchDefaultValue = false

    // parameters of xFunc annotation
    val xFuncAnnotationFieldNameAutoRefresh: TermName = newTermName("autoRefresh")

    val name: TermName = newTermName("name")
    // XXX Get rid of after PickledInputStream API is rationalized not to have temporalContext for events
    val unsafeValidTime: TermName = newTermName("unsafeValidTime")
    val validTime: TermName = newTermName("validTime")
    val storeContext: TermName = newTermName("storeContext")
    val loadContext: TermName = newTermName("loadContext")
    val transactionTime: TermName = newTermName("transactionTime")
    val lookupSrc: TermName = newTermName("lookupSrc")

    // Local variable names
    val tmp: TermName = newTermName("tmp$")
    val arg: TermName = newTermName("arg$") // Generic argument name
    val key: TermName = newTermName("k$")
    val ec: TermName = newTermName("ec")
    val when: TermName = newTermName("when")

    val flags: TermName = newTermName("flags")

    val gen: TermName = newTermName("gen")
    val value: TermName = newTermName("value")
    val pss: TermName = newTermName("pss")
    val get: TermName = newTermName("get")
    val apply: TermName = newTermName("apply")
    val copy: TermName = newTermName("copy")
    val iterator: TermName = newTermName("iterator")

    val bynameDummy: TermName = newTermName("<by-name wrapper>")

    val ignoreWithin: TermName = newTermName("IGNORE_WITHIN")
    val mock: TermName = newTermName("mock")
  }

  object entityMetaDataAnnotationNames {
    // we have EntityMetaDataAnnotation in scope as a val as well in names
    import optimus.platform.annotations.internal.{EntityMetaDataAnnotation => DEF}
    val slotNumber: TermName = newTermName(DEF.name_slotNumber)
    val explicitSlotNumber: TermName = newTermName(DEF.name_explicitSlotNumber)
    val isStorable: TermName = newTermName(DEF.name_isStorable)
    val isTrait: TermName = newTermName(DEF.name_isTrait)
    val isObject: TermName = newTermName(DEF.name_isObject)
    val projected: TermName = newTermName(DEF.name_projected)
    val fullTextSearch: TermName = newTermName(DEF.name_fullTextSearch)
    val monoTemporal: TermName = newTermName(DEF.name_monoTemporal)
  }
  object embeddableMetaDataAnnotationNames {
    val isTrait: TermName = newTermName("isTrait")
    val isObject: TermName = newTermName("isObject")
    val projected: TermName = newTermName("projected")
  }
  object accelerateInfoAnnotationNames {
    val path: TermName = newTermName("path")
  }
  object tpnames {
    // optimus.platform (user code) annotations
    val asyncOff: TypeName = newTypeName("asyncOff")
    val backed: TypeName = newTypeName("backed")
    val embeddable: TypeName = newTypeName("embeddable")
    val entity: TypeName = newTypeName("entity")
    val executeColumn: TypeName = newTypeName("columnar")
    val event: TypeName = newTypeName("event")
    val handle: TypeName = newTypeName("handle")
    val impure: TypeName = newTypeName("impure")
    val sequential: TypeName = newTypeName("sequential")
    val indexed: TypeName = newTypeName("indexed")
    val key: TypeName = newTypeName("key")
    val loom: TypeName = newTypeName("loom")
    val loomOff: TypeName = newTypeName("loomOff")
    val node: TypeName = newTypeName("node")
    val async: TypeName = newTypeName("async")
    val job: TypeName = newTypeName("job")
    val recursive: TypeName = newTypeName("recursive")
    val elevated: TypeName = newTypeName("elevated")
    val loomNodes: TypeName = newTypeName("nodes")
    val loomLambdas: TypeName = newTypeName("lambdas")
    val loomLcn: TypeName = newTypeName("lcn")
    val loomImmutables: TypeName = newTypeName("immutables")
    val nodeLift: TypeName = newTypeName("nodeLift")
    val scenarioIndependent: TypeName = newTypeName("scenarioIndependent")
    val siRhs: TypeName = newTypeName("siRhs")
    val givenRuntimeEnv: TypeName = newTypeName("givenRuntimeEnv")
    val givenAnyRuntimeEnv: TypeName = newTypeName("givenAnyRuntimeEnv")
    val asyncLazyWithAnyRuntimeEnv: TermName = newTermName("asyncLazyWithAnyRuntimeEnv")
    val stable: TypeName = newTypeName("stable")
    val staged: TypeName = newTypeName("staged")
    val stored: TypeName = newTypeName("stored")
    val getter: TypeName = newTypeName("getter")
    val field: TypeName = newTypeName("field")
    val transient: TypeName = newTypeName("transient")
    val projected: TypeName = newTypeName("projected")
    val reified: TypeName = newTypeName("reified")
    val tailrec: TypeName = newTypeName("tailrec")
    val fullTextSearch: TypeName = newTypeName("fullTextSearch")

    // internal annotations
    val defNodeCreator: TypeName = newTypeName("defNodeCreator")
    val defAsyncCreator: TypeName = newTypeName("defAsyncCreator")
    val valNodeCreator: TypeName = newTypeName("valNodeCreator")
    val nodeSync: TypeName = newTypeName("nodeSync")
    val miscFlags: TypeName = newTypeName("miscFlags")
    val tweakable: TypeName = newTypeName("tweakable")
    val nonTweakable: TypeName = newTypeName("nonTweakable")
    val propertyAccessor: TypeName = newTypeName("propertyAccessor")
    val internalApi: TypeName = newTypeName("internalApi")
    val entersGraph: TypeName = newTypeName("entersGraph")
    val storedVar: TypeName = newTypeName("storedVar")
    val autoGenCreation: TypeName = newTypeName("autoGenCreation")
    val c2p: TypeName = newTypeName("c2p")
    val EntityMetaDataAnnotation: TypeName = newTypeName("EntityMetaDataAnnotation")
    val EmbeddableMetaDataAnnotation: TypeName = newTypeName("EmbeddableMetaDataAnnotation")
    val AccelerateInfoAnnotation: TypeName = newTypeName("AccelerateInfoAnnotation")

    val Args: TypeName = newTypeName("Args")
    val PropType: TypeName = newTypeName("PropType")

    val EntityReference: TypeName = newTypeName("EntityReference")
    val EventInfo: TypeName = newTypeName("EventInfo")
    val ClassEntityInfo: TypeName = newTypeName("ClassEntityInfo")
    val ModuleEntityInfo: TypeName = newTypeName("ModuleEntityInfo")
    val Embeddable: TypeName = newTypeName("Embeddable")
    val Entity: TypeName = newTypeName("Entity")
    val EntityInfo: TypeName = newTypeName("EntityInfo")
    val InlineEntity: TypeName = newTypeName("InlineEntity")
    val Storable: TypeName = newTypeName("Storable")
    val Key: TypeName = newTypeName("Key")
    val KeyImpl: TypeName = newTypeName("KeyImpl")
    val UniqueKeyImpl: TypeName = newTypeName("UniqueKeyImpl")
    val NonUniqueKeyImpl: TypeName = newTypeName("NonUniqueKeyImpl")
    val NonUniqueEventKeyImpl: TypeName = newTypeName("NonUniqueEventKeyImpl")
    val PropertyInfo: TypeName = newTypeName("PropertyInfo")
    val ReallyNontweakablePropertyInfo: TypeName = newTypeName("ReallyNontweakablePropertyInfo")
    val BaseIndexPropertyInfo: TypeName = newTypeName("BaseIndexPropertyInfo")
    val IndexPropertyInfo: TypeName = newTypeName("IndexPropertyInfo")
    val IndexPropertyInfoForErefFilter: TypeName = newTypeName("IndexPropertyInfoForErefFilter")
    val UniqueIndexPropertyInfo: TypeName = newTypeName("UniqueIndexPropertyInfo")
    val EventIndexPropertyInfo: TypeName = newTypeName("EventIndexPropertyInfo")
    val EventUniqueIndexPropertyInfo: TypeName = newTypeName("EventUniqueIndexPropertyInfo")
    val NodeTaskInfo: TypeName = newTypeName("NodeTaskInfo")
    val GenPropertyInfo: TypeName = newTypeName("GenPropertyInfo")

    val Node: TypeName = newTypeName("Node")
    val NodeFuture: TypeName = newTypeName("NodeFuture")
    val NodeKey: TypeName = newTypeName("NodeKey")
    val CompletableNode: TypeName = newTypeName("CompletableNode")
    val PropertyNode: TypeName = newTypeName("PropertyNode")
    val EvaluationContext: TypeName = newTypeName("EvaluationContext")
    val ScenarioStack: TypeName = newTypeName("ScenarioStack")
    val Scenario: TypeName = newTypeName("Scenario")
    val TemporalContext: TypeName = newTypeName("TemporalContext")
    val QueryTemporality: TypeName = newTypeName("QueryTemporality")
    val TimeSlice: TypeName = newTypeName("TimeSlice")
    val Instant: TypeName = newTypeName("Instant")
    val PropInfoHelper: TypeName = newTypeName("PropInfoHelper")
    val Lazy: TypeName = newTypeName("Lazy")

    val EventCompanionBase: TypeName = newTypeName("EventCompanionBase")
    val EntityCompanionBase: TypeName = newTypeName("EntityCompanionBase")
    val EmbeddableCompanionBase: TypeName = newTypeName("EmbeddableCompanionBase")
    val EmbeddableTraitCompanionBase: TypeName = newTypeName("EmbeddableTraitCompanionBase")
    val KeyedEntityCompanionBase: TypeName = newTypeName("KeyedEntityCompanionBase")
    val KeyedEntityCompanionCls: TypeName = newTypeName("KeyedEntityCompanionCls")

    val Pickler: TypeName = newTypeName("Pickler")
    val PickledOutputStream: TypeName = newTypeName("PickledOutputStream")
    val Unpickler: TypeName = newTypeName("Unpickler")
    val PickledInputStream: TypeName = newTypeName("PickledInputStream")
    val PickledMapWrapper: TypeName = newTypeName("PickledMapWrapper")

    val CopyHelper: TermName = newTermName("CopyHelper")

    val BusinessEvent: TypeName = newTypeName("BusinessEvent")
    val BusinessEventImpl: TypeName = newTypeName("BusinessEventImpl")
    val ContainedEvent: TypeName = newTypeName("ContainedEvent")
    val Manifest: TypeName = newTypeName("Manifest")
    val Option: TypeName = newTypeName("Option")
    val Left: TypeName = newTypeName("Left")
    val Right: TypeName = newTypeName("Right")
    val LocationTag: TypeName = newTypeName("LocationTag")

    val ElevatedDistributor: TermName = newTermName("ElevatedDistributor")

    val UnsupportedOperationException: TypeName = newTypeName("UnsupportedOperationException")

    val tmp: TypeName = newTypeName("_$")

    lazy val needGetterAnnos: Set[Name] = Set(tpnames.node, tpnames.scenarioIndependent, tpnames.indexed)
    lazy val needFieldAnnos: Set[Name] = Set(tpnames.stored)
  }

  object suffixes {
    val NODE_CLASS = "$node"
    val NODE_QUEUED = "$nodeQueued"
    val WITH_VALUE = "$withValue"
    val PROPERTY_CLASS = "$propertyInfo"
    val KEY_INFO = "$keyInfo"
    val IMPL = "$impl"
    val BACKING = "$backing"
    val VAR_HANDLE = "$vh"

    val CREATE_NODE_NAME = "$newNode"
    val CREATE_NODE_NAME_TN: TermName = newTermName(CREATE_NODE_NAME)
    val INITIALIZER = "$init"
    val MEMBER_DESC = "$md"
    val REIFIED = "$reified"
    val ASYNCLAZY = "$asyncLazy"
  }

  private def makeCapitalized(str: String) = {
    val charArray = str.toCharArray
    charArray(0) = Character.toUpperCase(charArray(0))
    new String(charArray)
  }

  // Mangle names
  def mkBackedNodeName(name: Name): TermName = newTermName("backed$" + name)
  def mkGetNodeName(name: Name): TermName = newTermName("" + name + "$queued")

  def isCreateNodeName(name: Name): Boolean = name.endsWith(suffixes.CREATE_NODE_NAME_TN)
  def mkCreateNodeName(name: Name): TermName = newTermName("" + name + suffixes.CREATE_NODE_NAME)
  def nodeNameFromCreateNodeName(name: Name): Name = name stripSuffix suffixes.CREATE_NODE_NAME_TN
  def mkWithNodeName(name: Name): TermName = newTermName("" + name + "$withNode")
  def mkPropertyInfoName(infoHolder: TypedUtils#PropertyInfoWrapping, name: Name): TermName =
    mkPropertyInfoName(infoHolder.propertyPrefix, name, infoHolder.onModule)
  def mkPropertyInfoName(prefix: String, name: Name, onModule: Boolean): TermName =
    newTermName(prefix + name + (if (onModule) "_info" else ""))
  def mkPropertyInfoForwarderName(name: Name, onModule: Boolean): TermName =
    newTermName("" + name + (if (onModule) "_info" else ""))
  def mkKeyInfoName(name: Name): TermName = newTermName("" + name + suffixes.KEY_INFO)
  def mkImplName(name: Name): TermName = newTermName("" + name + suffixes.IMPL)
  def mkVarHandleName(name: Name): TermName = newTermName("" + name + suffixes.VAR_HANDLE)
  def mkBackingName(name: Name): TermName = newTermName("" + name + suffixes.BACKING)
  def mkGetByName(name: Name): TermName = newTermName("getBy" + makeCapitalized(name.toString))
  def mkGetOptionByName(name: Name): TermName = newTermName("getOptionBy" + makeCapitalized(name.toString))
  def mkNodeTraitName(name: Name): TypeName = newTypeName("" + name + suffixes.NODE_CLASS)
  def mkDerivedTagName(name: Name): TermName = newTermName("" + name + "$tag")
  def mkLocationTagName(name: Name): TermName = newTermName("" + name + "$LT")
  def mkNonRTName(name: Name): TermName = newTermName("" + name + "$nonRT")

  def mkTweakHandlerName(name: TermName): Name with TermName = name.append(names.tweakHandlerSuffix)
  def isTweakHandler(name: Name): Boolean = name.endsWith(names.tweakHandlerSuffix)

  def mkEntPropInitializerName(name: Name): TermName = newTermName("" + name + suffixes.INITIALIZER)
}
