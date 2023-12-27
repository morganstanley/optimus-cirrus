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
package optimus.tools.scalacplugins.entity.reporter

import optimus.tools.scalacplugins.entity.OptimusPhaseInfo
import optimus.tools.scalacplugins.entity.OptimusPhases

object OptimusErrors extends OptimusErrorsBase with OptimusPluginAlarmHelper {
  // api check
  val DEPRECATING_USAGE =
    error1(20510, OptimusPhases.APICHECK, "Usage @deprecating(\"msg\", \"optimus.platform.Foo,optimus.dal\"): %s")
  val AT_NODE_NOT_SUPPORTED_FOR_CONSTRUCTORS =
    error0(20511, OptimusPhases.APICHECK, "@node or @async annotation not supported on constructors")
  // following alarm is to prevent usage of JSR310 outside of the cases where we have to convert to/from JSR310 at
  // call-sites to external libraries (trend jars for example)
  val JSR310_USAGE =
    error0(
      20512,
      OptimusPhases.APICHECK,
      "Use of JSR310 (javax.time) is deprecated. Use java.time " +
        "instead. For JSR310 types used in external library APIs, use optimus.utils.datetime.JSR310Conversions " +
        "implicits to convert to and from java.time at call-sites"
    )

  val PLATFORM_ONLY =
    error1(
      20514,
      OptimusPhases.APICHECK,
      "%s may not be used outside of optimus.platform and related platform code"
    )

  val INCORRECT_ADVANCED_USAGE =
    error1(
      20515,
      OptimusPhases.APICHECK,
      "Incorrect usage of %s.  You probably should not be using it at all."
    )

  val NOWARN = error1(20516, OptimusPhases.APICHECK, "Illegal use of @nowarn: %s")

  // auto async
  val UNABLE_TO_PARALLELIZE_COLLECTION =
    warning1(20553, OptimusPhases.AUTOASYNC, "Unable to parallelize %s; must explicitly choose apar or aseq")
  val UNMARKED_ASYNC_CLOSURE_T =
    warning1(
      20554,
      OptimusPhases.AUTOASYNC,
      "Illegal async operation over %s; Only RT calls found so .apar (rather than .aseq) should be appropriate."
    )
  val UNMARKED_ASYNC_CLOSURE_D =
    warning1(
      20555,
      OptimusPhases.AUTOASYNC,
      "Illegal async operation over %s; Explicit @async or (potentially) non-RT calls found, so consider whether .aseq (vs .apar) is necessary."
    )
  val UNMARKED_ASYNC_CLOSURE_A =
    warning1(
      20556,
      OptimusPhases.AUTOASYNC,
      "Illegal async operation over %s; Possible non-RT calls found,so  consider whether .aseq (vs .apar) is necessary."
    )

  // optimus_valaccessors phase errors
  val TRANSFORM_ERROR_AUTOASYNC = error1(20557, OptimusPhases.AUTOASYNC, "Error in autoasync transform:\n%s")

  val FAILED_CONVERSION =
    error3(20558, OptimusPhases.AUTOASYNC, "Failed to convert call to %s with async closure to call to %s due to %s")

  val NO_ASNODE_VARIANT_FOUND =
    error1(20559, OptimusPhases.AUTOASYNC, "No asNode/asAsync variant found for %s")

  val ERROR_LOCATION_TAG_GENERATION =
    error1(20560, OptimusPhases.GENERATE_LOCATION_TAG, "Error in generating location tag transform:\n%s")

  val ERROR_LOCATION_TAG_MATCHING_METHOD = error2(
    20561,
    OptimusPhases.GENERATE_LOCATION_TAG,
    "There needs to be exactly 1 matching method but found %s. Method name: %s")

  val ERROR_LOCATION_TAG_MUST_BE_RT = error1(
    20562,
    OptimusPhases.GENERATE_LOCATION_TAG,
    "This method call is not being assigned to a an entity field. " +
      "Thus, it must have a matching non-RT method, but no such matching method found. Method name: %s"
  )

  // Unclassified transformation errors
  val TRANSFORM_ERROR_ADJUST_AST = error2(21000, OptimusPhases.ADJUST_AST, "Error transforming in %s: %s")

  val CHILD_PARENT_WITH_KEY_INDEX =
    error0(21001, OptimusPhases.ADJUST_AST, "@stored(childToParent=true) val must not be @key/@indexed")
  val KEY_INDEX_MUST_STABLE = error0(
    21002,
    OptimusPhases.ADJUST_AST,
    "@key/@indexed members must be stable (non-@node or @node(tweak=false) or final @node val) "
  )
  val KEY_INDEX_WITH_PRIVATE =
    error0(21003, OptimusPhases.ADJUST_AST, "Illegal private[this] @key/@indexed (if ctor param, add 'val')")
  val MULTI_PRIMARY_KEY = error2(21004, OptimusPhases.ADJUST_AST, "%s \"%s\" cannot have multiple non-secondary keys")
  val UNIQUE_INDEX_OF_EVENT = error1(21005, OptimusPhases.ADJUST_AST, "Event \"%s\" cannot have unique indexes")
  val KEY_INDEX_IN_OBJ_ENTITY = error0(21006, OptimusPhases.ADJUST_AST, "Illegal @key/@indexed on @entity object.")
  val KEY_INDEX_MUST_PARAMLESS_DEF =
    error0(21007, OptimusPhases.ADJUST_AST, "Only a parameterless def can be marked with @key or @indexed.")
  val CANT_GENERATE_INDEX = error1(21008, OptimusPhases.ADJUST_AST, "Cannot generate an index from %s")
  val UNSUPPORTED_KEY_INDEX_SPEC = error1(
    21009,
    OptimusPhases.ADJUST_AST,
    "Unsupported entry in @key/@index def specification. Expected: identifier, found: %s"
  )

  // These rules have either been removed or made more general - old values kept as comments for documentation purposes
  // val NO_INDEX_ON_EVENT_TRAIT = error0(1010, OptimusPhases.ADJUST_AST, "Unsupported indexes on event traits")
  // val NO_EVENT_INDEX_OVERRIDE = error0(1011, OptimusPhases.ADJUST_AST, "Unsupported overriding indexed/key fields")
  val NO_PRIVATE_EVENT_INDEX = error0(21012, OptimusPhases.ADJUST_AST, "Unsupported indexes on private event fields")
  val UNKNOWN_PARAM_OF_INDEXED =
    error1(21013, OptimusPhases.ADJUST_AST, "Unknown named parameter of @indexed annotation: %s")
  val UNNAMED_PARAM_OF_INDEXED =
    error0(21014, OptimusPhases.ADJUST_AST, "@indexed definition must use named arguments in annotation")
  // TODO (OPTIMUS-37938): move this to refchecks or somewhere so it can support @nowarn
  val NO_INDEX_OVERRIDE = warning0(21015, OptimusPhases.ADJUST_AST, "Unsupported overriding indexed/key fields")
  val NON_STORED_KEY_INDEX =
    error0(21016, OptimusPhases.ADJUST_AST, "Cannot have @key or @indexed on non-@stored @entity")

  val BACKED_IN_TRANSIENT = error0(21100, OptimusPhases.ADJUST_AST, "@backed can not be applied in a @transient entity")
  val BACKED_WITH_NODE = error0(21101, OptimusPhases.ADJUST_AST, "@backed requires a @node(tweak=true)")
  val BACKED_WITH_TRANSIENT = error0(21102, OptimusPhases.ADJUST_AST, "@backed are persistent not @transient")
  val BACKED_WITH_PARAMETERS = error0(21104, OptimusPhases.ADJUST_AST, "@backed can only be used on parameterless defs")
  val BACKED_WITH_TWEAKHANDLER = error0(21105, OptimusPhases.ADJUST_AST, "@backed invalidated by also set")
  val BACKED_VAL = error0(21107, OptimusPhases.ADJUST_AST, "Invalid use of @backed with val")
  val BACKED_IN_NON_ENTITY = error0(21108, OptimusPhases.ADJUST_AST, "@backed is only available in @entity classes")

  // removed: val NODE_IN_EVENT = error0(21200, OptimusPhases.ADJUST_AST, "@node is not supported on @event types")
  val NODE_WITH_FORMAL_PARAM = error0(
    21201,
    OptimusPhases.ADJUST_AST,
    "Illegal @node on formal param (did you forget val on a constructor param?)"
  )
  val NODE_WITH_PRIVATE_VAL = error0(21202, OptimusPhases.ADJUST_AST, "Illegal private[this] @node val")
  val NODE_WITH_PRIVATE_DEF = error0(21203, OptimusPhases.ADJUST_AST, "Illegal private[this] @node def")
  val NODE_WITH_VAR = error0(21204, OptimusPhases.ADJUST_AST, "@node not allowed on vars")
  val NODE_WITH_TWEAKHANDLER = error0(21205, OptimusPhases.ADJUST_AST, "@node val can't have a tweak handler")
  val TWEAKNODE_WITH_GENERIC_TYPE =
    error0(21206, OptimusPhases.ADJUST_AST, "@node with generic type can't be tweakable")
  val NODE_WITH_WRONG_ANNO = error1(21207, OptimusPhases.REF_CHECKS, "Invalid use of @node in conjunction with @%s")
  val TWEAKNODE_IN_NON_ENTITY =
    error0(21208, OptimusPhases.ADJUST_AST, "Invalid attempt to mark @node(tweak=true) on non-entity @node")
  val NODE_WITH_NAMED_PARAM =
    error0(21209, OptimusPhases.ADJUST_AST, "@node must have named parameter, @node(tweak = true/false)")

  val EVENT_WITH_ENTITY = error0(21300, OptimusPhases.ADJUST_AST, "Invalid simultaneous use of @event and @entity")
  val INLINE_WITH_ENTITY = error0(21301, OptimusPhases.ADJUST_AST, "@inline not supported on @entity object.")
  val LAZY_VAL_IN_DEFINITION =
    error1(
      21302,
      OptimusPhases.ADJUST_AST,
      "Illegal lazy val in %s definition. Refactor your code or try annotating with @givenRuntimeEnv"
    )
  val VAR_IN_ENTITY = error1(21303, OptimusPhases.ADJUST_AST, "Illegal var in %s definition.")
  val ENTITY_WITH_CASE_CLASS = error0(21304, OptimusPhases.ADJUST_AST, "Illegal @entity case class")
  val METHOD_LOCAL_ENTITY = error0(21305, OptimusPhases.ADJUST_AST, "Illegal method-local @entity definition.")
  val UNKNOWN_PARAM_OF_ENTITY =
    error1(21306, OptimusPhases.ADJUST_AST, "Unknown named parameter of @entity annotation: %s")
  val UNNAMED_PARAM_OF_ENTITY =
    error0(21307, OptimusPhases.ADJUST_AST, "@entity definition must use named arguments in annotation")
  val ENTITY_OBJ_WITH_SCHEMA_VERSION =
    error0(21308, OptimusPhases.ADJUST_AST, "@entity object can't have schemaVersion")
  val TRANSIENT_ENTITY_WITH_SCHEMA_VERSION =
    error0(21309, OptimusPhases.ADJUST_AST, "@transient @entity can't have schemaVersion")
  val ABSTRACT_ENTITY_WITH_SCHEMA_VERSION =
    error0(21310, OptimusPhases.ADJUST_AST, "only concrete @entity class can have schemaVersion")
  val NEG_SCHEMA_VERSION = error0(21311, OptimusPhases.ADJUST_AST, "schemaVersion can't be negative")
  val ENTITY_WITH_TRANSIENT = error0(21312, OptimusPhases.ADJUST_AST, "@transient can't be used with @entity")
  val STORED_ENTITY_WITH_ARGUMENT =
    error0(21313, OptimusPhases.ADJUST_AST, "@stored can't have arguments if used with @entity")
  val NO_MULTIPLE_INDEX =
    error1(21315, OptimusPhases.ADJUST_AST, "val or def %s cannot have multiple @key and @indexed annotations")
  // removed:
  //   val STORED_ENTITY_WITH_RECURSIVE_BOUNDED_GENERIC_TYPE =
  //     errorOptional0(21316, OptimusPhases.ADJUST_AST, "Generic @stored @entity cannot take recursive type boundary")
  //   val EVENT_WITH_RECURSIVE_BOUNDED_GENERIC_TYPE =
  //     error0(21317, OptimusPhases.ADJUST_AST, "Generic @event cannot take recursive type boundary")
  val UNKNOWN_PARAM_OF_STORED =
    error1(21318, OptimusPhases.ADJUST_AST, "Unknown named parameter of @stored annotation: %s")
  val UNNAMED_PARAM_OF_STORED =
    error0(21319, OptimusPhases.ADJUST_AST, "@stored definition must use named arguments in annotation")
  val ILLEGAL_IMPLICIT =
    error1(
      21320,
      OptimusPhases.ADJUST_AST,
      "Illegal implicit @%s declaration; write an explicit implicit conversion instead"
    )

  val EVENT_WITH_MULTI_ARGLIST =
    error0(21500, OptimusPhases.ADJUST_AST, "@event constructors must have exactly one arglist.")
  val EVENT_WITH_OBJECT = error0(21501, OptimusPhases.ADJUST_AST, "object @events are not supported")
  val EVENT_WITH_CASE_CLASS = error0(21502, OptimusPhases.ADJUST_AST, "Illegal @event case class")
  val EVENT_WITH_TRANSIENT = error0(21503, OptimusPhases.ADJUST_AST, " @event can't be @transient")
  // removed: val EVENT_INHERITED_MUST_BE_DECLARED = error0(21504, OptimusPhases.ADJUST_AST, "only @event can extends @event")
  val METHOD_LOCAL_EVENT = error0(21505, OptimusPhases.ADJUST_AST, "Illegal method-local @event definition.")
  val CONTAINED_EVENT_WITH_KEY_INDEX =
    error0(21506, OptimusPhases.REF_CHECKS, "@event(contained=true) must not contain @key/@indexed fields")
  val UNKNOWN_PARAM_OF_EVENT =
    error1(21507, OptimusPhases.ADJUST_AST, "Unknown named parameter of @event annotation: %s")
  val UNNAMED_PARAM_OF_EVENT = error0(
    21508,
    OptimusPhases.ADJUST_AST,
    "@event definition must use named arguments in annotation (contained or projected)"
  )
  val ABSTRACT_CONTAINED_EVENT = error0(
    21509,
    OptimusPhases.ADJUST_AST,
    "@event(contained=true) cannot be abstract"
  )

  val ASYNC_WITH_VAL = error0(21601, OptimusPhases.ADJUST_AST, "Illegal @async val, @async is only permitted with def")
  val NONENTITY_NODE_WITH_VAL = error0(
    21602,
    OptimusPhases.ADJUST_AST,
    "Illegal non-entity @node val, @node only works with def in non-entity types"
  )
  val ELEVATED_WITH_WRONG_ANNOTATION =
    error1(21603, OptimusPhases.REF_CHECKS, "Invalid use of @elevated in conjunction with @%s")
  val ASYNC_WITH_WRONG_ANNOTATION =
    error1(21604, OptimusPhases.REF_CHECKS, "Invalid use of @async in conjunction with @%s")
  val ELEVATED_WITH_ENTITY_OR_EVENT =
    error1(21605, OptimusPhases.ADJUST_AST, "Invalid use of @elevated in @%s (@elevated has the @async semantics)")
  val GIVEN_RUNTIME_ENV_MISUSE =
    error0(
      21606,
      OptimusPhases.ADJUST_AST,
      "@givenRuntimeEnv can only be used with @entity @node; @givenAnyRuntimeEnv can only be used with @async val in a non-@entity"
    )

  val STORED_WITH_PRIVATE = error0(21700, OptimusPhases.ADJUST_AST, "Illegal @stored on private[this] member.")
  val STORED_WITH_DEF = error0(21701, OptimusPhases.ADJUST_AST, "Illegal @stored def.")
  val STORED_MEMBER_ON_NON_STORED_OWNER =
    error0(21702, OptimusPhases.REF_CHECKS, "Can't have @stored members outside of @stored @entity or @event")
  val STORED_HAS_TRANSIENT_VAL =
    error0(21703, OptimusPhases.VAL_ACCESSORS, "Stored entities or events cannot have @transient fields")

  val IMPURE_WITH_HANDLER = error0(21800, OptimusPhases.ADJUST_AST, "Invalid use of @impure with @handle")
  // removed: val IMPURE_WITH_NODE = error0(21801, OptimusPhases.ADJUST_AST, "Invalid use of @impure with @node")
  val IMPURE_WITH_VAL = error0(21804, OptimusPhases.ADJUST_AST, "Illegal @impure val, @impure only work with def")
  val IMPURE_WITH_ENTERSGRAPH = error0(21805, OptimusPhases.ADJUST_AST, "Invalid use of @impure with @entersGraph")

  val HANDLER_WITH_VAL = error0(21850, OptimusPhases.ADJUST_AST, "Illegal @handle val, @handle only work with def")

  val PROJECTED_WITH_HANDLER = error0(21865, OptimusPhases.ADJUST_AST, "Invalid use of @projected with @handle")
  val PROJECTED_MUST_PARAMLESS_DEF =
    error0(21866, OptimusPhases.ADJUST_AST, "Only a def without parenthesis can be marked with @projected")
  val PROJECTED_INVALID_DEF = error0(
    21867,
    OptimusPhases.ADJUST_AST,
    "Invalid use of @projected on def. Consider either add @indexed or change rhs to embeddable attribute paths"
  )
  val STORED_PROJECTED_INVALID_VAL = error0(
    21868,
    OptimusPhases.ADJUST_AST,
    "please use @projected instead of @stored(projected = true) before val definition if you want use accelerator feature"
  )
  val PROJECTED_FIELD_MISSING =
    error0(21869, OptimusPhases.ADJUST_AST, "projected is set to be true but there is no stored field in embeddable")
  val PROJECTED_WITH_REIFIED = error0(21870, OptimusPhases.ADJUST_AST, "Invalid use of @projected with @reified")
  val REIFIED_WITH_TYPEARGS = error0(21871, OptimusPhases.ADJUST_AST, "Invalid use of @reified on def with type args")

  val FULLTEXTSEARCH_INVALID_USAGE = error1(
    21880,
    OptimusPhases.DAL_REF_CHECKS,
    "Invalid usage of @fullTextSearch %s -"
  )

  val VARIADIC_CTOR_PARAM = error1(
    21917,
    OptimusPhases.ADJUST_AST,
    "Unsupported variadic constructor parameter '%s' on @entity. Consider using a Seq instead."
  )
  val EMBEDDABLE_ONLY_WITH_CASE_CLASS =
    error0(21900, OptimusPhases.ADJUST_AST, "@embeddable is only supported on case classes and sealed traits")
  val EMBEDDABLE_ONLY_WITH_CASE_OBJECT = error0(
    21901,
    OptimusPhases.ADJUST_AST,
    "@embeddable object is not supported. Did you mean @embeddable case object?"
  )

  // removed: val LAZY_VALS_MUST_BE_SI = error0(21902, OptimusPhases.ADJUST_AST, "Async lazy vals must be @scenarioIndependent")

  val NO_DEFERRED_LAZY_VALS = error0(21903, OptimusPhases.ADJUST_AST, "Async lazy vals cannot be abstract")

  val EMBEDDABLE_SUBTYPE_NAME_CONFLICT = error2(
    20021,
    OptimusPhases.EMBEDDABLE,
    "Cannot define %s more than once which extend the same @embeddable trait %s"
  )
  val EMBEDDABLE_SUBTYPE =
    error2(20022, OptimusPhases.EMBEDDABLE, "Subtype %s of @embeddable type %s must be marked @embeddable")

  val SCENARIO_INDEPENDENT_CANT_TWEAK =
    error0(21906, OptimusPhases.ADJUST_AST, "Invalid use of scenarioIndependent on tweakable node")
  val EMBEDDABLE_AND_ENTITY =
    error0(21907, OptimusPhases.ADJUST_AST, "@embeddable/@stable cannot be applied on @entity object")
  val EMBEDDABLE_PRIVATE_CTOR_DEFAULTS =
    error1(
      21908,
      OptimusPhases.ADJUST_AST,
      "Default arguments are unsupported on @embeddable/@stable private constructors"
    )
  val ONLY_ALLOW_NODE_FOR_EMBEDDABLE_CTOR =
    error1(21910, OptimusPhases.ADJUST_AST, "Only @embeddable/@stable constructors are allowed to be @node")
  val ALL_EMBEDDABLE_CTOR_MUST_BE_NODE_IF_ONE_IS_NODE =
    error1(21911, OptimusPhases.ADJUST_AST, "All Embeddable/stable constructors must be @node if one is @node")
  val NO_READRESOLVE_OVERRIDDEN_IS_ALLOWED_IN_NODE_EMBEDDABLE =
    error1(21912, OptimusPhases.EMBEDDABLE, "No readResolve overridden is allowed to in @node embeddable/stable")

  // @embeddable are warnings ATM, but should become errors after we fix the code
  val NO_CUSTOM_METHOD_IN_EMBEDDABLE_CASE_CLASS =
    // TODO (OPTIMUS-30981): make this a warning1 later
    info1(21920, OptimusPhases.EMBEDDABLE, "@embeddable case classes should not declare custom %s methods")
  val NO_CUSTOM_METHOD_IN_STABLE_CASE_CLASS =
    error1(21921, OptimusPhases.EMBEDDABLE, "@stable case classes cannot declare custom %s methods")

  val NO_METHOD_IN_EMBEDDABLE_CASE_CLASS =
    // TODO (OPTIMUS-30981): make this a warning1 later
    info1(21922, OptimusPhases.EMBEDDABLE, "@embeddable case classes without %s methods")
  val NO_METHOD_IN_STABLE_CASE_CLASS =
    error1(21923, OptimusPhases.EMBEDDABLE, "@stable case classes without %s methods")

  val NOT_STABLE_AND_EMBBEDDABLE =
    error0(
      21924,
      OptimusPhases.EMBEDDABLE,
      "@embeddable case classes cannot be marked @stable (but we always treat them as stable)"
    )
  val STABLE_CASE_CLASS_NOT_FINAL =
    error0(
      21925,
      OptimusPhases.EMBEDDABLE,
      "@stable case class isn't final or effectively final (sealed with no subclases)"
    )
  val EMBEDDABLE_CASE_CLASS_NOT_FINAL =
    error0(
      21926,
      OptimusPhases.EMBEDDABLE,
      "@embeddable case class isn't final or effectively final (sealed with no subclases)"
    )

  val STABLE_CASE_CLASS_HAS_OUTER =
    error1(
      21927,
      OptimusPhases.EMBEDDABLE,
      "@stable case class has on outer reference that isn't a plain package or object - %s"
    )
  val EMBEDDABLE_CASE_CLASS_HAS_OUTER =
    error1(
      21928,
      OptimusPhases.EMBEDDABLE,
      "@embeddable case class has on outer reference that isn't a plain package or object - %s"
    )

  val STABLE_WITHOUT_PARAMS =
    error0(
      21929,
      OptimusPhases.EMBEDDABLE,
      "@stable case classes without params don't make sense. Make it a case object?"
    )
  val EMBEDDABLE_WITHOUT_PARAMS =
    // TODO (OPTIMUS-30981): make this a warning0 later
    info0(
      21930,
      OptimusPhases.EMBEDDABLE,
      "@embeddable case classes without params don't make sense. Make it a case object?"
    )

  val ILLEGAL_ANNOTATION = error2(
    21931,
    OptimusPhases.ADJUST_AST,
    "@%s is not permitted %s"
  )
  // TODO (OPTIMUS-31928): Delete this eventailly
  val EMBBEDDABLE_WITH_IGNORED =
    error0(21932, OptimusPhases.EMBEDDABLE, "@embeddable case classes cannot have @notPartOfIdentity parameters)")

  // propertyinfo phase errors
  val NODE_ARITY_LIMIT = error0(
    21620,
    OptimusPhases.PROPERTY_INFO,
    "Implementation restriction: @node defs may not have more than 21 parameters"
  )

  // propertyinfo phase errors
  val EMBEDDABLE_HAS_DEFAULT_UNPICKLEABLE_VALUE = error0(
    21621,
    OptimusPhases.REF_CHECKS,
    "HasDefaultUnpickleableValue type param should be of the same @embeddable sealed trait type for which companion has been defined."
  )

  // attachment phase errors
  val CLASSIFIER_TRANSFORM_ERROR = error1(20500, OptimusPhases.CLASSIFIER, "Unable to transform tree: %s\n")

  // refcheck phase errors
  val PROJECTED_NOTSET = error0(
    21942,
    OptimusPhases.DAL_REF_CHECKS,
    "projected is not set to be true but @projected is marked on some stored field"
  )
  val PROJECTED_ANNOTATION_MISSING = error0(
    21943,
    OptimusPhases.DAL_REF_CHECKS,
    "projected is set to be true but @projected is not marked on any stored field"
  )
  val PROJECTED_NONSTORED_PROPERTY =
    error0(21944, OptimusPhases.DAL_REF_CHECKS, "@projected cannot be applied to non-stored property")
  val PROJECTED_NONCTOR_PROPERTY = error0(
    21945,
    OptimusPhases.DAL_REF_CHECKS,
    "@projected must be applied to primary constructor properties for embeddable"
  )
  val PROJECTED_NOTSTORED_ENTITY_EVENT =
    error0(21946, OptimusPhases.REF_CHECKS, "only @stored entity/event class can be projected")
  val PROJECTED_NOTFINAL_EVENT =
    error0(21947, OptimusPhases.DAL_REF_CHECKS, "only final event class can be projected")
  val PROJECTED_INVALID_EMBEDDABLE =
    error0(21948, OptimusPhases.DAL_REF_CHECKS, "only final embeddable case class can be projected")
  val PROJECTED_INVALID_CLASS =
    error0(21949, OptimusPhases.DAL_REF_CHECKS, "only entity/event/embeddable class can be projected")
  val PROJECTED_INDEXED_UNSUPPORTED_PROPERTY = error0(
    21950,
    OptimusPhases.DAL_REF_CHECKS,
    "Remove (indexed=true) in @projected " +
      "since projected collection index can only be applied to collection of Primitive type, String, DateTime and Entity."
  )
  val PROJECTED_FIELD_UNSUPPORTED = error0(
    21951,
    OptimusPhases.DAL_REF_CHECKS,
    "Remove @projected since projected field can't be nested Option/Knowable type(e.g. Option[Option[_]] or Knowable[Option[_]])."
  )
  val PROJECTED_DEF_INVALID_RHS =
    error0(
      21952,
      OptimusPhases.DAL_REF_CHECKS,
      "def with @projected only supports embeddable attribute paths/types or simple compound field"
    )
  val PROJECTED_DEF_MUST_REFER_CTOR_PARAM = error0(
    21953,
    OptimusPhases.DAL_REF_CHECKS,
    "def with @projected should only refer to constructor parameters of @embeddables"
  )
  val PROJECTED_DEF_MUST_REFER_NONTWEAKABLE =
    error0(21954, OptimusPhases.DAL_REF_CHECKS, "def with @projected should only refer to non-tweakable property")
  val PROJECTED_INVALID_ENTITY =
    error0(21955, OptimusPhases.DAL_REF_CHECKS, "projected entity should be trait/concrete class")
  val PROJECTED_SLOT_MISSMATCH =
    error0(21956, OptimusPhases.DAL_REF_CHECKS, "slot of projected entity should be 0")

  val REIFIED_REF_REIFIED =
    error0(21970, OptimusPhases.REF_CHECKS, "@reified method cannot call other @reified methods")
  val REIFIED_WITH_TWEAKABLE =
    error0(21971, OptimusPhases.REF_CHECKS, "Invalid use of @reified with @node(tweak = true)")
  val REIFIED_INVALID_CLASS =
    error0(
      21972,
      OptimusPhases.REF_CHECKS,
      "@reified should be used on def of entity/event/embeddable, the def must be final"
    )

  val TWEAKNODE_WITH_COVARIANT_TYPE =
    error2(22001, OptimusPhases.REF_CHECKS, "Tweakable node with type %s references covariant type %s")
  val COPYABLE_WITH_COVARIANT_TYPE =
    warning2(22010, OptimusPhases.REF_CHECKS, "Copied entity val with type %s references covariant type %s")
  val CHILD_PARENT_MUST_BE_SET = error0(
    22002,
    OptimusPhases.REF_CHECKS,
    "@stored(childToParent=true) val must be an immutable.Set or optimus.platform.CovariantSet of an @stored @entity type"
  )
  val INDEX_MUST_EMBEDDABLE_PATH =
    error0(22003, OptimusPhases.REF_CHECKS, "@indexed only supported on embeddable attribute paths/types")
  val INDEX_MUST_REFER_CTOR_PARAM =
    error0(22004, OptimusPhases.REF_CHECKS, "Index may only refer to constructor parameters of @embeddables")
  val KEY_INDEX_MUST_STABLE2 =
    error0(22005, OptimusPhases.REF_CHECKS, "@key/@index members must be stable, non-@node values")
  val CANT_GENERATE_INDEX2 = error1(
    22006,
    OptimusPhases.REF_CHECKS,
    "Cannot generate an index from %s"
  ) // TODO (OPTIMUS-0000): duplicate error message in different phases, redundant check?
  val INDEX_SEQ_DEF_MAP_ON_ENTITY =
    error1(
      22007,
      OptimusPhases.REF_CHECKS,
      "The term %s is not valid on the RHS of an @indexed def. For more information " +
        "see http://codetree-docs/optimus/docs/OptimusCoreDAL/DalIndexing.html#rules-for-indexes"
    )
  val NONTWEAK_OVERRIDE_TWEAK =
    error2(22100, OptimusPhases.REF_CHECKS, "Illegal nontweakable override of tweakable property %s.%s")
  val DEFAULT_OVERRIDE_TWEAK =
    error2(22101, OptimusPhases.REF_CHECKS, "Illegal default @node override of tweakable property %s.%s")
  val TWEAK_OVERRIDE_NONTWEAK =
    error2(22102, OptimusPhases.REF_CHECKS, "Illegal tweakable override of nontweakable property %s.%s")
  val DEFAULT_OVERRIDE_NONTWEAK =
    error2(22103, OptimusPhases.REF_CHECKS, "Illegal default @node override of nontweakable property %s.%s")
  val NONSI_OVERRIDE_SI =
    error2(22105, OptimusPhases.REF_CHECKS, "Illegal override of @scenarioIndependent property %s.%s")
  val ASYNC_OVERRIDE_NODE = error0(22106, OptimusPhases.REF_CHECKS, "@async cannot override @node")
  val NONBACKED_OVERRIDE_BACKED =
    error0(22107, OptimusPhases.REF_CHECKS, "The base class defines the node as @backed, the child class must as well")
  val ILLEGAL_COVARIANT_OVERRIDE =
    error4(22108, OptimusPhases.REF_CHECKS, "Illegal covariant override of %s in %s: expected %s, got %s.")
  val NODE_OVERLOAD_ERROR =
    error1(22109, OptimusPhaseInfo.Namer, "Can't overload @node methods, see overload on line (%s)")
  val CREATE_NODE_TRAIT_OVERLOAD_ERROR =
    error1(22110, OptimusPhaseInfo.Namer, "Can't overload @createNodeTrait methods, see overload on line (%s)")
  val IMPLICIT_OVERRIDE_ERROR = error1(
    22111,
    OptimusPhases.REF_CHECKS,
    "Can't implicit override val (define a same name private val with parent class): %s"
  )
  val DEF_OVERRIDE_VAL = error2(22112, OptimusPhases.REF_CHECKS, "Can't override val with def: %s.%s")

  val LOSE_ANNO_WHEN_OVERRIDE =
    error3(22201, OptimusPhases.REF_CHECKS, "method %s with @%s can only be overridden by @%s")
  val ADD_ANNO_WHEN_OVERRIDE =
    warning3(22202, OptimusPhases.REF_CHECKS, "override non @%s method %s with @%s - this can cause a sync stack")
  // removed:
  //   val ADD_ASYNC_ANNO_WHEN_OVERRIDE = info3(
  //     22203,
  //     OptimusPhases.REF_CHECKS,
  //     "override non @%s method %s with @%s, the async transform won't happen when use base class's type")
  val IMPURE_IN_NODE = error2(22205, OptimusPhases.REF_CHECKS, "Invalid call to @impure method %s from RT context %s")

  val TRANSIENT_INHERIT =
    error2(22300, OptimusPhases.REF_CHECKS, "entity %s must be @transient as supertype %s is marked @transient")
  val ENTITY_EXTEND_EMBEDDABLE = error2(22301, OptimusPhases.REF_CHECKS, "@entity %s cannot extend @embeddable %s")
  val ENTITY_EXTEND_CASE = error2(22302, OptimusPhases.REF_CHECKS, "@entity %s cannot extend case %s")
  val ENTITY_EXTEND_NONENTITY =
    error2(22303, OptimusPhases.REF_CHECKS, "@entity class %s cannot extend non-entity class %s")
  val NONSTORABLE_EXTEND_STORABLE = error5(
    22304,
    OptimusPhases.REF_CHECKS,
    "%s inherits from an %s class or trait, must also be marked %s (%s, %s)"
  ) // redundant check as 2303?
  val SOAP_OBJ_MUST_ENTITY = error0(22305, OptimusPhases.REF_CHECKS, "A @soapobject must be an @entity object.")
  val STORABLE_WITH_DELAYED_INIT =
    error1(22306, OptimusPhases.REF_CHECKS, "Invalid use of %s on a subclass of DelayedInit (e.g. LegacyOptimusApp)")
  val METHOD_LOCAL_ENTITY2 =
    error0(22307, OptimusPhases.REF_CHECKS, "Illegal method-local @entity definition") // redundant ?
  val ENTITY_WITH_EMBEDDABLE = error0(
    22308,
    OptimusPhases.REF_CHECKS,
    "one class can't be @embeddable @entity, you only can use one of them"
  ) // lift to adjust_ast phase?
  val INNER_ENTITY_MUST_TRANSIENT = error1(22309, OptimusPhases.REF_CHECKS, "inner class %s must be @transient")
  val ENTITY_WITH_WRONG_TYPE = error0(
    22310,
    OptimusPhases.REF_CHECKS,
    "@entity only supported on class or module def"
  ) // lift to adjust_ast phase?
  val EMBEDDABLE_WITH_WRONG_TYPE = error0(
    22311,
    OptimusPhases.REF_CHECKS,
    "@embeddable only supported on case class or object def"
  ) // lift to adjust_ast phase?
  val ENTITY_WITH_MULTI_DEFAULT_KEY =
    error2(22312, OptimusPhases.REF_CHECKS, "Entity has multiple default @keys, inheriting %s from %s")
  val EMBEDDABLE_INHERITED_VAL_OR_VAL =
    error2(22313, OptimusPhases.REF_CHECKS, "inherited val/var %s from %s (vals must be defined in constructor)")
  val INNER_EVENT = error0(22314, OptimusPhases.REF_CHECKS, "Illegal inner @event definition.")
  val ENTITY_EXTEND_NONENTITY_STRONG = warning3(
    22315,
    OptimusPhases.REF_CHECKS,
    "@entity class %s cannot extend non-entity class %s that contains vals or vars (%s)"
  )
  val EVENT_EXTEND_NONEVENT = error3(
    22316,
    OptimusPhases.REF_CHECKS,
    "@event class %s cannot extend non-event class %s that contains non-abstract methods (%s)"
  )
  val EVENT_WITH_WRONG_TYPE =
    error0(22317, OptimusPhases.REF_CHECKS, "@event only supported on class or module def") // lift to adjust_ast phase?

  val ENTITY_OBJECT_EXTEND_NONENTITY_STRONG = warning3(
    22318,
    OptimusPhases.REF_CHECKS,
    "@entity object %s cannot extend non-entity class %s that contains non-abstract methods (%s)"
  )
  val ENTITY_STORED_EXTENDS_NONENTITY_STRONG = warning3(
    22319,
    OptimusPhases.REF_CHECKS,
    "@stored @entity class %s cannot extend non-entity class %s that contains non-abstract methods (%s)"
  )
  val ENTITY_PROPERTY_INFO_ACCIDENTAL_OVERRIDE = error3(
    22320,
    OptimusPhases.REF_CHECKS,
    "implementation restriction: %s extending its companion %s leads to a name clash due to the synthetic PropertyInfo %s generated in the object. Please rename the object or its parent."
  )
  val CONTAINED_EVENT_EXTEND_CONTAINED_EVENT = error3(
    22321,
    OptimusPhases.REF_CHECKS,
    "@event(contained=true) class %s cannot extend @event(contained=true) class"
  )
  val REFLECTIVE_NODE_INVOCATION =
    error1(
      22359,
      OptimusPhases.REF_CHECKS,
      "implementation restriction: illegal reflective call to structural node/async method %s"
    )

  val LOCAL_NODE = error0(22400, OptimusPhases.REF_CHECKS, "Illegal local @node definition")
  val NOT_SUPPORTED_NODE =
    error0(22401, OptimusPhases.REF_CHECKS, "@node not supported on value class or universal trait")
  val LOCAL_ASYNC = error0(22402, OptimusPhases.REF_CHECKS, "Illegal local @async definition")

  val UNKNOWN_KEY_USAGE = error0(22500, OptimusPhases.REF_CHECKS, "I can't figure out what @key is doing here.")
  val INDEX_ON_NESTED_COLLECTION =
    error0(22501, OptimusPhases.REF_CHECKS, "@indexed can't be used on nested collection property fields")

  val SOAP_MUST_BE_HANDLE =
    error0(22900, OptimusPhases.REF_CHECKS, "A method exposed via Optimus SOAP must be an @handle.")
  val USER_BAD_ENTITY_CREATER = error4(
    22901,
    OptimusPhases.REF_CHECKS,
    s"User-defined %s.%s cannot call %s.%s: Crossing the streams."
  ) // what's this for?
  val BANED_METHOD = error1(22902, OptimusPhases.REF_CHECKS, "Use of %s in this project is denied")
  val UPCAST_WITH_WRONG_TYPE =
    error0(22903, OptimusPhases.REF_CHECKS, "@upcasting only supported on concrete @entity class or @event class")
  val NEW_ENTITY = error1(22904, OptimusPhases.REF_CHECKS, "Entity instances are not allocated, use %s instead")
  val USE_TRANSFORMER = error3(
    22905,
    OptimusPhases.REF_CHECKS,
    "@stored @entity with a schema version cannot have default values or $init methods for values. Class %s has a val %s which is in error %s"
  )
  val ENTITY_OBJECT_WITH_STORED = error1(22906, OptimusPhases.REF_CHECKS, "@stored can't be used with @entity object")
  val NEW_EVENT =
    error1(22908, OptimusPhases.REF_CHECKS, "Event instances are not allocated, use %s.uniqueInstance instead")

  val XFUNC_AUTO_REFRESH_ON_NON_NODE =
    error0(22955, OptimusPhases.REF_CHECKS, "@xFunc can only have autoRefresh=true for a @node")
  // TODO (OPTIMUS-12546): ban @handle with @xFunc
  // lazy val XFUNC_WITH_HANDLE = error0(XXX, OptimusPhases.REF_CHECKS, "@handle cannot be @xFunc")

  val RETURN_IN_NODE =
    error1(22959, OptimusPhases.REF_CHECKS, "@node/@async %s cannot contain return expressions")

  val FORBIDDEN_PARAM_NAME =
    error2(
      22909,
      OptimusPhases.REF_CHECKS,
      "Due to implementation limitations, @node/@createNodeTrait method %s cannot contain a forbidden parameter name %s"
    )
  val NO_UNIQUE_INDEXED_COLLECTION =
    error1(22910, OptimusPhases.REF_CHECKS, "Unique Indexes not allowed for Collection types")

  val ITERATOR_ESCAPES =
    warning2(22911, OptimusPhases.REF_CHECKS, "Escaping iterator of type %s. %s")

  // optimus_valaccessors phase errors
  val TRANSFORM_ERROR3 = error1(
    25000,
    OptimusPhases.VAL_ACCESSORS,
    "Error in redirect accessors transform:\n%s"
  ) // remove such kind of error? throw exception instead?

  // optimus_entityinfo phase errors
  val TRANSFORM_ERROR4 = error1(
    26000,
    OptimusPhases.ENTITY_INFO,
    "Error in entity info transform:\n%s"
  ) // remove such kind of error? throw exception instead?
  val KEY_SHADOWED =
    error2(26001, OptimusPhases.ENTITY_INFO, "Entity/event %s inherits conflicting definitions of keys/indexes:\n\t%s")
  val INLINE_ENTITY_WITH_INDEX_KEY =
    error0(26002, OptimusPhases.ENTITY_INFO, "@inline entities may not have (possibly inherited) keys or indexes.")
  val INDEX_ADDED_TO_EXISTING_STORED_VAL = warning2(
    26003,
    OptimusPhases.ENTITY_INFO,
    "Entity/event %s adds an index/key to fields that has its storage determined by type %s"
  )

  // node lift phase errors
  val TRANSFORM_ERROR5 = error2(
    26050,
    OptimusPhases.NODE_LIFT,
    "Error transforming in %s: %s"
  ) // remove such kind of error? throw exception instead?
  val LIFT_NONTWEAK = error1(26051, OptimusPhases.NODE_LIFT, "%s is not tweakable")
  val LIFT_NONNODE = error2(26052, OptimusPhases.NODE_LIFT, "%s.%s is not a @node")
  val LIFT_NONPROP =
    error2(26053, OptimusPhases.NODE_LIFT, "%s.%s is not a property node (it must be declared in an entity)")

  val NODELIFT_NOMATCH = error1(26100, OptimusPhases.NODE_LIFT, "nodeLift: Could not find matching %s")
  val NODELIFT_UNSUPPORT = error1(26101, OptimusPhases.NODE_LIFT, "nodeLift: Unsupported construct %s")
  val USE_TWEAK_OPT_DIRECT = error0(26102, OptimusPhases.NODE_LIFT, "tweak operator cant be used directly")
  val NODELIFT_NOMATCH_VALUE2TT = error1(
    26103,
    OptimusPhases.NODE_LIFT,
    "nodeLift: type mismatch between value2TweakTarget and %s: do not import optimus.platform.value2TweakTarget to obtain the correct diagnostic"
  )
  val NODELIFT_NONTWEAK_VALUE2TT = error1(
    26104,
    OptimusPhases.NODE_LIFT,
    "nodeLift: type mismatch between value2TweakTarget and %s: do not import optimus.platform.value2TweakTarget to obtain the correct diagnostic"
  )

  // optimus_generatenodemethods phase errors
  val GENERATENODES_TRANSFORM_ERROR =
    error1(
      26500,
      OptimusPhases.GENERATE_NODE_METHODS,
      "Unable to transform tree: %s\n"
    ) // remove such kind of error? throw exception instead?

  // async graph phase errors
  val ASYNC_TRANSFORM_ERROR = warning2(
    27000,
    OptimusPhases.ASYNC_GRAPH,
    "Error in async transform of %s (rewriting may be necessary): %s"
  ) // remove such kind of error? throw exception instead?

  val LOCAL_TYPER_ERROR = error3(27001, OptimusPhases.ASYNC_GRAPH, "tpe: %s\nLast block\n%s\n%s")

  val WRONG_TWEAK_HANDLER_RETURN_TYPE = error1(
    27100,
    OptimusPhases.ASYNC_GRAPH,
    "Incorrect tweak handler return type: required scala.Seq[optimus.platform.Tweak], found %s"
  )
  val WRONG_TWEAK_HANDLER_PARAM_TYPE =
    error2(27102, OptimusPhases.ASYNC_GRAPH, "Incorrect tweak handler parameter type(s): required (%s), found (%s)")
  val TWEAK_BYNAME_IN_NODE = error1(
    27004,
    OptimusPhases.ASYNC_GRAPH,
    "Using Tweak.byName inside @node or @async will cause runtime issues (memory leak, can't dist), use Tweak.byValue or remove @node from %s"
  )

  val UNHANDLED_FUNCTION_PATTERN = error1(
    27200,
    OptimusPhases.ASYNC_GRAPH,
    "Can't recognize function pattern; try to rewrite with explicit closure arguments: %s"
  )
  val NODELIFT_ETA_EXPANSION = error2(
    27201,
    OptimusPhases.ASYNC_GRAPH,
    """Eta-expansion of this expression transformed it into a block. This can have surprising semantics.
      |Consider creating an explicit function by %s, or move bound subexpression(s) into vals: %s
      """.stripMargin
  )

  // pickling phase errors
  // removed:
  //   val ENTITY_CTOR_NEED_PARAM_LIST =
  //     error0(28000, OptimusPhases.OPTIMUS_CONSTRUCTORS, "At least one parameter list is required for an Entity ctor")
  //   val NO_PICKLED_INPUT_STREAM =
  //     error0(28001, OptimusPhases.OPTIMUS_CONSTRUCTORS, "Cannot find PickledInputStream parameter for an Entity ctor")
  //   val ILLEGAL_CTOR_PARAM_USAGE = error0(
  //     28002,
  //     OptimusPhases.OPTIMUS_CONSTRUCTORS,
  //     "Illegal use of constructor parameter in storable subclass of nonstorable class")
  //   val LOCAL_TYPED_ERROR = error1(28003, OptimusPhases.OPTIMUS_CONSTRUCTORS, "Exception building constructor: %s")
  //   val NON_VAL_PARAM_IN_STORABLE_ENTITY = error0(
  //     28004,
  //     OptimusPhases.OPTIMUS_CONSTRUCTORS,
  //     "Using non-val ctor argument in @stored @entity initialization is not allowed")
  val INVALID_CONSTRUCTOR_STATEMENT = error0(
    28005,
    OptimusPhases.OPTIMUS_CONSTRUCTORS,
    "Invalid statement in @stored @entity or @event constructor.  Only require, assert, and val assignments are allowed."
  )

  // safe interop export errors
  val EXPORT_NON_NODE_NON_ASYNC =
    error1(29001, OptimusPhases.SAFE_EXPORT_CHECK, "%s is exported, and is neither annotated with @node nor @async")
  val EXPORT_PRIVATE = error1(29002, OptimusPhases.SAFE_EXPORT_CHECK, "%s is exported, but is not public")
  val EXPORT_DUPLICATE_PARAM =
    error2(29003, OptimusPhases.SAFE_EXPORT_CHECK, "%s is exported, but parameter %s is a duplicate ProgressReporter")
  val EXPORT_WRONG_PARAM = error3(
    29004,
    OptimusPhases.SAFE_EXPORT_CHECK,
    "%s is exported but its parameter %s of type %s is not serialisable, a scala primitive, or a ProgressReporter"
  )
  val EXPORT_WRONG_RETRUN_TYPE = error2(
    29005,
    OptimusPhases.SAFE_EXPORT_CHECK,
    "%s is exported but its return type %s is not serialisable or a scala primitive"
  )
  val EXPORT_WRONG_LOCATION = error1(
    29006,
    OptimusPhases.SAFE_EXPORT_CHECK,
    "%s is exported, but it is in a wrong location. It has to be @node/@async method on a reflectable, TOP-LEVEL module i.e. object (Have you exported a method on a class? Have you exported a node of an inner object?)"
  )
  val EXPORT_VAL_VAR_PARAM =
    error1(29007, OptimusPhases.SAFE_EXPORT_CHECK, "%s is exported, but vals/vars/params can't be exported")
  val EXPORT_CLASSES = error1(
    29008,
    OptimusPhases.SAFE_EXPORT_CHECK,
    "%s is exported, but classes/traits/objects can't be exported, only @node/@async defs on a module (scala object)"
  )
  val EXPORT_DEFAULT_ARG =
    error2(29009, OptimusPhases.SAFE_EXPORT_CHECK, "%s is exported, but contains parameter %s with default value")
  // default error message

  // @job-related [[JOB_EXPERIMENTAL]]
  val JOB_WITH_VAL = error0(29106, OptimusPhases.ADJUST_AST, "@job cannot be set on vals")
  val JOB_ENTITY_OR_NONOBJECT =
    error0(29107, OptimusPhases.ADJUST_AST, "@job may be only set within a non-entity object")
  val JOB_SINGLEPARAM_NONSTOREDENTITY =
    error0(29108, OptimusPhases.REF_CHECKS, "@job may only have one single parameter of type @stored @entity")
  val JOB_WITHOUT_NODE_ASYNC = error0(29109, OptimusPhases.ADJUST_AST, "@job may be only set on a @node / @async")
  val NON_TOP_LEVEL_JOB_OWNER = error0(
    29110,
    OptimusPhases.PROPERTY_INFO,
    "@job cannot be defined on object living within nested classes / objects")

  val POISONED_PLACEHOLDER =
    error1(29200, OptimusPhases.POSITION, "A placeholder method wasn't replaced during compilation: %s")

  val INCORRECT_META_FIELDS =
    error1(
      29201,
      OptimusPhases.EXPORTINFO,
      "This Catalog/Owner %s needs to be defined outside of your project, preferably in the datacatalog_api project, this error can also be triggered if you're passing a function, a class, or any other way that alters the structure. Please refer to DefaultTradeCatalog or DefaultOwner for an example"
    )
}

object OptimusNonErrorMessages extends OptimusNonErrorMessagesBase with OptimusPluginAlarmHelper {

  val INTERNAL_COMPILE_INFO = info1(10000, OptimusPhases.APICHECK, "Compilation information: %s")
  val UNUSED_ALSO_SET = error2(10409, OptimusPhases.REF_CHECKS, "Unused also-set %s, %s")
  val DEPRECATING = warning2(10500, OptimusPhases.APICHECK, "%s is deprecated: %s")
  val DEPRECATING_LIGHT = warning2(10501, OptimusPhases.APICHECK, "%s is deprecated: %s")

  val UNUSED_NOWARN = error1(10502, OptimusPhases.APICHECK, "Unused or redundant @nowarn: %s")
  val NOWARN_DEPRECATION =
    warning2(
      10503,
      OptimusPhases.APICHECK,
      "Suppressing a deprecation warning for %s. Please contact owners of the %s scope for reviews.")

  val INCORRECT_ADVANCED_USAGE_LIGHT =
    warning1(10510, OptimusPhases.APICHECK, "Incorrect usage of %s.  You probably should not be using it at all.")

  val AUTO_ASYNC_OFF = info0(10553, OptimusPhases.AUTOASYNC, "Not asyncing collection, due to asyncOff request.")
  val ASYNC_CONTAINS_INCOMPATIBLE =
    info0(10554, OptimusPhases.AUTOASYNC, "Not asyncing, due to problematic constructs in closure.")
  val ASYNC_WRAPPING_DEBUG = debug2(10555, OptimusPhases.AUTOASYNC, "Wrapping %s with %s.")
  val POSSIBLE_NON_RT = info2(
    10556,
    OptimusPhases.AUTOASYNC,
    "Possible non-RT operation %s in async closure of %s; consider when choosing aseq/apar."
  )
  val VAR_IN_ASYNC_CLOSURE = info0(10557, OptimusPhases.AUTOASYNC, "Possible reference to var")
  val UNNECESSARY_ASYNC_DEBUG = debug0(10558, OptimusPhases.AUTOASYNC, "Unnecessarily marked async")
  val VIEW_WITH_ASYNC = error0(10559, OptimusPhases.AUTOASYNC, "Views are not compatible with async collections.")
  val AUTO_ASYNC_DEBUG = debug1(10560, OptimusPhases.AUTOASYNC, "AutoAsync: %s")
  val MOCK_ENTITY = info0(10561, OptimusPhases.AUTOASYNC, "Possible application of mocking library to an entity.")
  val MOCK_NODE = info0(10562, OptimusPhases.AUTOASYNC, "Mocking libraries should not be used with node methods.")
  val MANUAL_ASYNC_COLLECT_APAR =
    info0(
      10563,
      OptimusPhases.AUTOASYNC,
      "Iterable.collect must by async'd manually.(No non-RT calls detected; .apar is likely appropriate.)"
    )
  val ASYNC_TRY = warning0(10564, OptimusPhases.AUTOASYNC, "Do not use try/catch with async body.")
  val MANUAL_OPTION_ASYNC_COLLECT =
    error0(10565, OptimusPhases.AUTOASYNC, "Option.collect must by async'd manually.")
  val MANUAL_ASYNC_COLLECT_ASEQ =
    info0(
      10566,
      OptimusPhases.AUTOASYNC,
      "Iterable.collect must by async'd manually.(Possible non-RT calls detected; .aseq may be necessary.)"
    )
  val ASYNC_CLOSURE =
    warning1(10567, OptimusPhases.AUTOASYNC, "Async call found in closure passed as sync argument to %s")
  val ASYNC_CLOSURE_AUTO =
    info1(
      10568,
      OptimusPhases.AUTOASYNC,
      "Async call found in closure passed as sync argument to auto-async'd %s"
    )

  val UNHANDLED_COMBINATOR_DEBUG = debug1(10569, OptimusPhases.AUTOASYNC, "Unhandled combinator transformation %s")
  val SUPRESS_ASYNC_LABEL_DEBUG =
    debug0(10570, OptimusPhases.AUTOASYNC, "Suppressing async labeling of code within suppressed auto-async block")
  val ASYNC_LAZY =
    info0(
      10571,
      OptimusPhases.AUTOASYNC,
      "Lazy val initialization with async causes a sync stack. Refactor your code or try annotating with @givenAnyRuntimeEnv @async"
    )
  val POSSIBLE_ASYNC_DEBUG = debug1(10572, OptimusPhases.AUTOASYNC, "Possibly async symbols: %s")
  val INLINING_DEBUG = debug2(10573, OptimusPhases.AUTOASYNC, "Inlining %s.%s")
  val FUNCTION_CONVERSION =
    debug2(10574, OptimusPhases.AUTOASYNC, "Converted call to %s with async closure to call to %s")
  val FUNCTION_CONVERSION_CANDIDATE_REJECTED =
    debug2(10575, OptimusPhases.AUTOASYNC, "Rejected auto-conversion of call to %s with async closure to call to %s")
  val ASYNC_CLOSURE_ERROR =
    warning1(10576, OptimusPhases.AUTOASYNC, "Async call found in closure passed as sync argument to %s")
  val TRY_CATCH_NODE =
    warning1(10577, OptimusPhases.AUTOASYNC, "try/catch with potentially non-RT exception: %s")
  val STOP_ADDING_ASYNC_VAL_EVERYWHERE =
    info0(
      10578,
      OptimusPhases.AUTOASYNC,
      "Potentially unused @givenAnyRuntimeEnv. See http://codetree-docs/optimus/docs/CoreAnnotations/AnnotationGivenAnyRuntimeEnv.html for details."
    )

  val ARTIFACT_ASYNC_CLOSURE =
    debug2(10588, OptimusPhases.AUTOASYNC, "Unpicking artifact %s: %s")

  // optimus refcheck node type warnings
  val MUTABLE_NODE_PARAMS =
    warning0(10600, OptimusPhases.REF_CHECKS, "Using mutable (e.g Array) types as node parameters")
  val MUTABLE_NODE_RETTYPES =
    warning0(10601, OptimusPhases.REF_CHECKS, "Using mutable (e.g Array) types as node return type")

  // adjust_ast phase warnings
  val NODE_WITH_NONTWEAKABLE_VAL =
    warning0(11000, OptimusPhases.ADJUST_AST, "Consider removing @node from non-tweakable final vals")
  val COMPOUND_KEY_MUST_DEF =
    warning0(11001, OptimusPhases.ADJUST_AST, "compound @key definitions should be defs")

  // refcheck phase warnings
  val TRANSIENT_CTOR_PARAM_USE = warning0(
    12000,
    OptimusPhases.REF_CHECKS,
    "Accessing a ctor parameter in a non-transient entity is unsafe. A JVM default value would be used if expression is evaluated. (That may also happen if a stored node is not found in the serialized stream)"
  )
  val SI_CALL_NONSI =
    warning2(
      12002,
      OptimusPhases.REF_CHECKS,
      "Call to (possibly) scenario dependent node %s from scenario independent context %s. Any tweak access will cause a runtime exception!"
    )
  val SI_CALL_NONSI_NOT_NODE =
    warning2(
      12003,
      OptimusPhases.REF_CHECKS,
      "Call to (possibly scenario dependent async function %s from a scenario independent context %s. Any tweak access will cause a runtime exception!"
    )

  // This should be an error!
  val TWEAK_IN_SI_CONTEXT =
    warning2(
      12004,
      OptimusPhases.REF_CHECKS,
      "Tweak %s cannot be resolved in a scenario-independent context %s. If the context is an SI node, you'll get a crash. If it's an entity constructor, you'll get UNDEFINED BEHAVIOUR."
    )

  val IMPURE_IN_NODE = info2(12205, OptimusPhases.REF_CHECKS, "Invalid call to @impure method %s from RT context %s")

  // optimus_valaccessors phase warnings
  val RELY_ON_BROKEN_BEHAVIOUR = warning2(
    15000,
    OptimusPhases.VAL_ACCESSORS,
    "This code was relying on broken behaviour. To retain the old behaviour change: %s to super.%s"
  )

  val DEALIASING = info2(
    15001,
    OptimusPhases.VAL_ACCESSORS,
    "Removing alias of %s to tweakable %s"
  )

  // optimus_storedprops phase warnings
  val ILLEGAL_TWEAK_BODY = warning1(
    16000,
    OptimusPhases.REF_CHECKS,
    "Illegal use of %s as tweak body!  (use transactionTimeNow, validTimeNow, or storeContextNow)"
  )

  // async graph phase warnings
  val CALL_ON_GRAPH_IN_CTOR =
    warning0(17000, OptimusPhases.ASYNC_GRAPH, "Cannot call onto graph from object constructor")
  val CALL_ASYNC_FROM_SYNC =
    warning1(
      17001,
      OptimusPhases.ASYNC_GRAPH,
      "Calling async function %s from sync context. " +
        "This causes sync stacks, which can have a severe performance impact. See the documentation here: http://codetree-docs/optimus/docs/OptimusNodes/SyncStacks.html. " +
        "Use @entersGraph ONLY if this is a legitimate graph entry point (not reachable from @node/@async). Otherwise, fix " +
        "the sync stack by adding @node or @async, or contact the graph team for help."
    )
  val CANNOT_ASYNC = warning1(17002, OptimusPhases.ASYNC_GRAPH, "Cannot make statement asynchronous: %s")
  val UNNECESSARY_TWEAK_BYNAME =
    error0(17003, OptimusPhases.ASYNC_GRAPH, "Unnecessary tweak byName, use Tweak.byValue instead")
  val PARTICULARLY_PERNICIOUS_USE_OF_ASYNC_LAMBDA_IN_CTOR =
    error0(
      17004,
      OptimusPhases.ASYNC_GRAPH,
      "Do not use apar/aseq in a val in an object. This is a deadlock waiting to happen."
    )

  val DEAD_NODE_CALL =
    warning1(17005, OptimusPhases.ASYNC_GRAPH, "Dead code: result of call to @node%s is not used.")

  val CODE_MOTION = debug1(17006, OptimusPhaseInfo.ScalaAsync, "Code motion: %s")

  // location tag phase warnings
  val LOCATION_TAG_INVALID_USE =
    warning0(17100, OptimusPhases.GENERATE_LOCATION_TAG, "Invalid use of location tag")

  val GENPROPERTYINFO_FALLBACK =
    info2(18000, OptimusPhaseInfo.NoPhase, "Falling back to GenPropertyInfo for %s on %s")

  val NOPOSITION = info1(19009, OptimusPhases.POSITION, "INFO: %s is not positioned")

  val POTENTIALLY_BROKEN_PROPERTY_TWEAKING =
    warning4(
      16002,
      OptimusPhases.ENTITY_INFO,
      "%s in class %s is NOT property tweakable through \n  %s := ... \nbecause its concrete implementation is from a mixin trait. %s"
    )

  // removed:
  //   val EMBEDDABLE_APPLY_REMOVAL = info1(
  //     19010,
  //     OptimusPhases.ADJUST_AST,
  //     "The auto-generated apply method for @node constructor of %s is removed since you have defined your customized apply")
}
