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

object VersioningAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.VERSIONING_MACRO

  val IDENTICAL_SOURCE_DESTINATION = warning0(
    81000,
    "The source and destination types are structurally identical. This transformer will never be executed.")
  val KEY_TYPE_MISMATCH = abort2(82000, "The @key properties of %s and %s must match.")
  val WRONG_FORM_EXPR = abort1(82001, "Expression must be a function of the form '(_: %s) => new { ... }'.")
  val INCORRECT_FUNC_PARAM =
    abort1(82002, "Incorrect function parameter specification. Expected something like '(x: %s)'.")
  val MALFORMED_RHS =
    abort0(82003, "Malformed right hand side. Should be an anonymous class declaration like 'new { ... }'.")
  val INVALID_DECL =
    abort1(82004, "The declaration of %s is invalid -- only stored vals can be declared in the anonymous class.")
  val STORED_VALS_MISSING_FROM_ANNO =
    abort2(82005, "The following fields in %s are not declared in the anonymous class: %s.")
  val BACKED_DEFS_MISSSING_FROM_ANNO =
    abort2(82006, "The following backed defs in %s are not declared in the anonymous class: %s.")
  val STORED_VALS_MISSING_FROM_DEST =
    abort2(82007, "The following fields in the anonymous class are not declared in %s: %s.")
  val BACKED_DEFS_MISSING_FROM_DEST =
    abort2(82008, "The following backed defs in the anonymous class are not declared in %s: %s.")
  val UNKNOWN_EXPR = abort1(82009, "The expression %s is too complicated.  To use it an unsafeTransformer is required.")
  val NO_MAPPING_FILED_IN_ANNO =
    abort2(82010, "The value of field %s in %s is not mapped to any field in the anonymous class.")

  val INVALID_TYPE =
    abort1(83001, "Type %s is not valid. Valid types are concrete entity/event types and refinements of AnyRef.")
  val NOT_IN_STORABLE_OR_EMBEDDABLE_COMPANION =
    abort0(83002, "Transformers must be defined within the companion object of an entity/event/embeddable.")
  val INVALID_TYPES = abort1(
    83003,
    "Type %s is not valid. Valid types are concrete entity/event/embeddable case classes and refinements of AnyRef.")

  val WRONG_TRANSFORMER_STEPS_BODY_EXPR = abort1(
    85000,
    "'transformerSteps/transformer' body expression must be a code block of the form ' { add[Int]('a, 12); remove('b); rename('c, 'd); ... }'; found: %s"
  )
  val TRANSFORMER_STEPS_LITERAL_SYMBOL_AND_ARGUMENT = abort0(
    85002,
    "Only constant arguments are allowed in transformerSteps. For e.g. 'add[Int]('a, 12)', 'valueOf[String]('comment)' etc., where 'a, 'comment and 12 are literal constants."
  )
  val TRANSFORMER_COPY_UNTOUCHED_FIELD_ALLOWED_ONLY_ONCE =
    abort0(85003, "In 'transformer' body, 'copyRemainingFields' step is allowed only once.")
  val REMOVE_FIELDS_NOT_FOUND_IN_SOURCE_TYPE =
    abort2(85004, "You are removing field(s) %s, but they're not present in source type %s")
  val REMOVE_FIELDS_FOUND_IN_DEST_TYPE =
    abort2(85005, "You are removing field(s) %s, but they're present in destination type %s")
  val ADD_FIELDS_PRESENT_IN_SOURCE_TYPE =
    abort2(85006, "You are adding new field(s) %s in destination type, but they're already present in source type %s")
  val ADD_FIELDS_NOT_PRESENT_IN_DEST_TYPE = abort2(
    85007,
    "You are adding new field(s) %s in destination type, but they're not present in the declaration of dest type %s")
  val MULTIPLE_TRANSFORMER_STEPS_FOR_FIELDS = abort0(
    85008,
    "There seems to be multiple steps defined for some field(s). 'tranformer' must have at most one step defined for a field.")
  val RENAME_FIELDS_NOT_PRESENT_IN_SOURCE_TYPE =
    abort2(85009, "You are renaming some field(s) %s, but they're not present in source type %s")
  val RENAME_FIELDS_NOT_PRESENT_IN_DEST_TYPE =
    abort2(85010, "You are renaming some field(s) to %s, but they're not present in destination type %s")
  val NO_STEPS_DEFINED_FOR_FIELDS = abort1(
    85011,
    "There are no steps defined for fields %s in destination type, you may want to just copy them using 'copyFields' or 'copyRemainingFields'?")
  val ADD_FIELD_TYPE_DEST_TYPE_MISMATCH =
    abort2(85012, "You are adding new field of type %s, but %s found in destination type declaration.")
  val VALUE_OF_FIELD_TYPE_SRC_TYPE_MISMATCH =
    abort2(85013, "You are reading a field using 'valueOf' of type %s, but %s found in source type declaration.")
  val VALUE_OF_FIELD_NOT_PRESENT_IN_SRC_TYPE =
    abort2(85014, "You are reading a field(s) %s using 'valueOf', but it is not present in source type declaration %s.")
  val COPY_FIELDS_NOT_FOUND_IN_SOURCE_TYPE =
    abort2(85015, "You are copying field(s) %s, but they're not present in source type %s")
  val COPY_FIELDS_NOT_FOUND_IN_DEST_TYPE =
    abort2(85016, "You are copying field(s) %s, but they're not present in destination type %s")
  val CHANGE_FIELDS_NOT_PRESENT_IN_SOURCE_TYPE =
    abort2(85017, "You are changing some field(s) %s, but they're not present in source type %s")
  val CHANGE_FIELDS_NOT_PRESENT_IN_DEST_TYPE =
    abort2(85018, "You are changing some field(s) to %s, but they're not present in destination type %s")
  val CHANGE_FIELD_TYPE_DEST_TYPE_MISMATCH =
    abort2(85019, "You are changing field to type %s, but %s found in destination type declaration.")
  val INVALID_STEP_IN_EMBEDDABLE = abort0(
    85020,
    "Invalid transformation step for embeddable. Embeddables only support 'add' and 'copy' transformation steps.")
  val NOT_SUPPORTED_TRANSFORMER_IN_EMBEDDABLE = abort0(85021, "UnsafeTransformers are not supported for embeddables.")

  val INVALID_MINUS_OPERATION_FOR_TYPES = abort2(
    86000,
    "Invalid operation 'typeA -:- typeB' between shapes %s. { %s } fields missing from the left operand or do you meant 'typeB -:- typeA?")
}
