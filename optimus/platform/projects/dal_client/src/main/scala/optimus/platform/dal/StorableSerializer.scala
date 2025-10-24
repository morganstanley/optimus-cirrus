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
package optimus.platform.dal

import optimus.platform.TemporalContext
import optimus.platform._
import optimus.platform.pickling.PickledProperties
import optimus.platform.storable.StorableReference
import optimus.platform.versioning.Shape
import optimus.platform.versioning.TransformerRegistry
import scala.util.control.NonFatal

private[optimus] trait StorableSerializer {
  @async final def version(
      className: String,
      properties: PickledProperties,
      context: TemporalContext,
      applyForceTransformation: Boolean = true): PickledProperties = {
    val afterForcedTransform =
      if (applyForceTransformation) TransformerRegistry.executeForcedReadTransform(className, properties, context)
      else properties
    val canonicalShape = TransformerRegistry.getCanonicalShape(className)
    val versioned =
      if (canonicalShape.isDefined) {
        val targetShape = canonicalShape.get
        // We use isCanonical = false here. We can do that safely because it's in the second parameter list of the Shape and therefore not
        // included in the equality check on a Shape. But that's fragile really (or at least, hacky) and ought to be fixed.
        val readShape = Shape.fromProperties(className, afterForcedTransform)
        TransformerRegistry
          .version(afterForcedTransform, readShape, targetShape, context)
          .getOrElse(afterForcedTransform)
      } else {
        afterForcedTransform
      }
    versioned
  }

  def handleDeserializationExceptions[A](
      className: String,
      properties: PickledProperties,
      versionedProperties: PickledProperties,
      ref: StorableReference)(f: => A): A = {
    try {
      f
    } catch {
      case NonFatal(ex) =>
        val shapeAsRead = Shape.fromProperties(className, properties)
        val shapeAfterVersioning = Shape.fromProperties(className, versionedProperties)
        val message = {
          val msg =
            s"Cannot deserialize entity/business event/embeddable/stored object with reference $ref/$className. The properties map is $properties. The shape as read is: $shapeAsRead."
          if (shapeAsRead == shapeAfterVersioning) msg
          else s"$msg The shape after versioning is: $shapeAfterVersioning."
        }
        TransformerRegistry.dumpState()
        throw new IncompatibleVersionException(message, ex)
    }
  }
}
