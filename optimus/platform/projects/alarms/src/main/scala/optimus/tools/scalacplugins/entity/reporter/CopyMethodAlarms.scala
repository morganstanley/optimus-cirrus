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

object CopyMethodAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.COPYMETHOD_MACRO

  val NO_SUCH_PROPERTY = error2(91001, "%s is not a val or def of %s")
  val NOT_PUBLIC_PROPERTY = error1(91002, "%s isn't public property, which can't be changed")
  val NOT_VAL_PROPERTY = error1(91003, "%s doesn't appear to be a val")
  val NO_SUCH_MEMBER = error2(91004, "%s is not a member of %s")
  val NOT_STORABLE_PROPERTY = error1(91005, "%s is not a stored property")
  val COPY_ASIS_PROPERTY = error1(91006, "%s is copyAsIs, which can't be changed")
  val TYPE_MISMATCH = error2(91007, "type mismatch, expected %s, got %s")

  val TRANSIENT_ENTITY = error1(92000, "@transient entity can't use %s")
  val NO_PUBLIC_CTOR = error0(92001, "entity doesn't have public constructor")

  val NEED_NAMED_PARAM = abort1(93000, "%s only support named parameters, please specify which property to change")
  val NEED_SECOND_ARG_LIST = error0(93001, "DAL.modify needs second arglist to tell what to change")
  val TOO_MANY_ARGS = abort2(93002, "too many args for %s, try to invoke %s with named parameters")

  val EVENT_COPY_CANNOT_CHANGE = abort2(94000, "%s can't change property %s")
  val UNKNOWN_EVENT_COPY_METHOD = abort1(94001, "unknown method %s called for @event")

  val TWEAKABLE_NODE_FN_OF_FIELD_WRONG_LAMBDA =
    abort1(95000, "Can only bind fieldName using syntax ofField(_.fieldName), but found %s")
  val TWEAKABLE_NODE_FN_OF_FIELD_NO_COPY_METHOD =
    abort1(95001, "Could not find copy method on %s (it should be a case class or have a manually written copy method)")
  val TWEAKABLE_NODE_FN_OF_FIELD_NO_COPY_PARAM = abort2(95002, "Could not find parameter %s on $s.copy method")
}
