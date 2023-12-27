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

import optimus.tools.scalacplugins.entity.StagingPhase

object CodeStyleErrors extends OptimusErrorsBase with OptimusPluginAlarmHelper {
  val RETURN_STATEMENT =
    error0(20011, StagingPhase.STANDARDS, s"Return statement is not needed here")
}

object CodeStyleNonErrorMessages extends OptimusNonErrorMessagesBase with OptimusPluginAlarmHelper {
  val CLASS_EXTENDS_APP = warning1(
    10001,
    StagingPhase.POST_TYPER_STANDARDS,
    "Concrete classes cannot extend OptimusApp or App, make %s into an object")

  val NON_FINAL_INNER_CASE_CLASS =
    warning0(
      10002,
      StagingPhase.POST_TYPER_STANDARDS,
      "Case classes encapsulated in other classes should be moved to the outermost containing class's companion object and marked final in most cases:  http://codetree-docs/optimus/docs/QualityAssurance/ReviewRules.html#non-final-case-class"
    )

  val NON_FINAL_CASE_CLASS = warning0(
    10003,
    StagingPhase.POST_TYPER_STANDARDS,
    "Case classes should be marked final in most cases:  http://codetree-docs/optimus/docs/QualityAssurance/ReviewRules.html#non-final-case-class"
  )

  val DISCOURAGED_CONSTRUCT =
    warning2(10006, StagingPhase.POST_TYPER_STANDARDS, "%s is discouraged because %s")

  val MOCK_FINAL_CASE_CLASS =
    warning0(10007, StagingPhase.POST_TYPER_STANDARDS, "Don't mock final case classes")
}

object Scala213MigrationMessages extends OptimusErrorsBase with OptimusPluginAlarmHelper {
  val TO_CONVERSION_TYPE_ARG =
    error0(
      20301,
      StagingPhase.POST_TYPER_STANDARDS,
      "For Scala 2.13 compatibility, replace `to[X]` by `to(X)`; this requires an `import scala.collection.compat._`"
    )

  val PREDEF_FALLBACK_STRING_CBF =
    error0(
      20302,
      StagingPhase.POST_TYPER_STANDARDS,
      """The implicit Predef.fallbackStringCanBuildFrom is used here.
        |This implicit instance is inferred when a `Seq` (or an unspecified) CanBuildFrom is required, but it builds an `IndexedSeq`.
        |Instead, use an explicit conversion / CanBuildFrom for List, IndexedSeq or Vector.
        |Enable "Show Implicit Hints" in IntelliJ to display the implicit argument.""".stripMargin
    )

  val VIEW_BOUND =
    error0(
      20303,
      StagingPhase.POST_TYPER_STANDARDS,
      "View bounds <% are deprecated in 2.13. `def f[T <% B]` is equivalent to `def f[T](implicit ev: T => B)`."
    )

  val NILARY_INFIX =
    error0(
      20304,
      StagingPhase.POST_TYPER_STANDARDS,
      """Methods with an empty parameter list cannot be called infix. Examples:
        |  - `obj operation ()`           -> `obj.operation()`
        |  - `obj toString`               -> `obj.toString`
        |  - `sequence { op } { op } end` -> `sequence { op } { op }.end""".stripMargin
    )

  val PROCEDURE_SYNTAX =
    error0(
      20305,
      StagingPhase.POST_TYPER_STANDARDS,
      "Procedure syntax `def f { statements() }` is deprecated, use an explicit return type instead: `def f: Unit = { statements() }`."
    )

  val NILARY_OVERRIDE =
    error2(
      20306,
      StagingPhase.POST_TYPER_STANDARDS,
      "Inconsistent override: the overridden method %s is defined %s"
    )

  val AUTO_APPLICATION =
    error1(
      20307,
      StagingPhase.POST_TYPER_STANDARDS,
      "Auto-application to `()` is deprecated. Supply the empty argument list `()` explicitly to invoke method %s, or remove the empty argument list from its definition.")

  val NULLARY_IN_213 =
    error1(
      20308,
      StagingPhase.POST_TYPER_STANDARDS,
      "Method %s doesn't have a parameter list in Scala 2.13. Remove the empty argument list () for cross-building."
    )

  val MAP_CONCAT_WIDENS =
    error2(
      20309,
      StagingPhase.POST_TYPER_STANDARDS,
      """The key type of the argument of ++ is not a sub-type of the receiver's key type. Type inference will differ in Scala 2.13. Remedies:
        |  - widen the the key type of the declaration of the receiver
        |  - Use `m1.toSeq ++ m2` if a Map is not needed
        |  -`import optimus.utils.CollectionUtils._` and use m1 +~+ m2 or Map.fromAll(m1, m2) to replicate the Scala 2.12 behaviour
        |Receiver key type: %s, Argument key type: %s.""".stripMargin
    )

  val EXPLICIT_CBF_ARGUMENT =
    warning0(
      20310,
      StagingPhase.POST_TYPER_STANDARDS,
      """Explicit arguments for the `CanBuildFrom` parameter are not supported. Collection operations in Scala 2.13 no longer have such a parameter.""".stripMargin
    )

  val DOUBLE_BUILDER_PLUSEQ =
    error0(
      20311,
      StagingPhase.POST_TYPER_STANDARDS,
      """Calling `OptimusDoubleBuilder.+=` boxes the argument double value, use `addOne` instead.""".stripMargin
    )

  val INT_TO_FLOAT =
    warning3(
      20312,
      StagingPhase.POST_TYPER_STANDARDS,
      """Widening conversion from %s to %s is deprecated because it loses precision, add an explicit `%s`.""".stripMargin
    )

  val INTEGRAL_DIVISION_TO_FLOATING =
    warning1(
      20313,
      StagingPhase.POST_TYPER_STANDARDS,
      """Integral division is implicitly converted (widened) to a floating point value. Add an explicit `.%s`.""".stripMargin
    )

  val NEEDS_UNSORTED =
    warning0(
      20314,
      StagingPhase.POST_TYPER_STANDARDS,
      "Transforming a Sorted Set/Map to and unsorted Set/Map now requires explicit demarcation with coll.unsorted.map; this requires an `import optimus.scalacompat.collection._"
    )
  val IMPORT_PARCOLLECTIONS =
    warning0(
      20315,
      StagingPhase.POST_TYPER_STANDARDS,
      "For cross-building with Scala 2.13, using `.par` requires adding `import optimus.scalacompat.collection.ParCollectionConverters._`"
    )
}

object StagingErrors extends OptimusErrorsBase with OptimusPluginAlarmHelper {

  // staging phase errors
  val STAGING_DEPRECATED = error1(
    20001,
    StagingPhase.STAGING,
    "Staging marker %s is deprecated and will be removed.  Please remove staging block."
  )
  val INVALID_STAGING_PREDICATE = error1(20002, StagingPhase.STAGING, "invalid staging predicate: %s")
  val MUTIPLE_STAGING_OBJECT =
    error0(20003, StagingPhase.STAGING, "staging imports only support one import for each object")

  // code-standards phase errors
  val NO_PACKAGE_OBJECT_IMPORT =
    error0(
      20010,
      StagingPhase.STANDARDS,
      "Do not import paths containing explicit package objects (just remove .`package`)"
    )

  val NO_COLLECTION_WILDCARD_IMPORT =
    error0(
      20012,
      StagingPhase.STANDARDS,
      "Use named imports for the collection package (collection.immutable) instead of the wildcard import collection._"
    )

  val AUGMENT_STRING =
    warning0(
      20013,
      StagingPhase.POST_TYPER_STANDARDS,
      "Suspicious use of implicit Predef.augmentString. Use .toSeq if you really want to treat it as a Seq[Char]. Consider if surrounding code should be a map rather than flatMap. Prefer + rather than ++ for String concatenation."
    )
}

object StagingNonErrorMessages extends OptimusNonErrorMessagesBase with OptimusPluginAlarmHelper {
  // staging phase warnings
  val UNKNOWN_STAGING_MARKER = error1(10004, StagingPhase.STAGING, "Unknown staging marker type: %s")

  // code-standards phase warnings
  val UNTYPED_IMPLICIT =
    warning1(
      10005,
      StagingPhase.POST_TYPER_STANDARDS,
      "Implicit definition should have explicit type (inferred %s)")
}
