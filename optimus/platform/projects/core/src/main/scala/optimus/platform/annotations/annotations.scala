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
package optimus.platform.annotations

import scala.annotation.meta._
import scala.annotation.StaticAnnotation
import scala.annotation.Annotation

class backingStore extends StaticAnnotation

/**
 * Marks a (closure) parameter of a method to have its environment captured by value rather than by using $outer
 * references. See CaptureByValueComponent for more details. You must mark both the method *and* the parameter.
 *
 * Note that since CaptureByValueComponent phase runs after asyncgraph, if you have a foo and foo$withNode pair, the
 * annotations need to go on foo$withNode not on foo.
 */
class captureByValue extends StaticAnnotation

/**
 * Turn off an enclosing @nodeLiftByName, if any.
 *
 * Use case: Tweaks(Tweak.byValue(foo.a := bar))
 */
class nodeLiftByValue extends StaticAnnotation

/**
 * Used by compiler plugin to produce the 2c. transformation of @nodeLift which generates a full PropertyNode rather
 * than a raw Node. This is used for @node on @stored @entity. must be applied in conjunction with @nodeLiftByName.
 */
class propertyNodeLift extends StaticAnnotation

/*
 * Used by compiler plugin to produce the following transformation at call site:
 *    @nodeLiftQueued def method[T](arg1 [, arg2]) -> method$nodeQueued[T](arg1$queued [, arg2$queued])
 */
class nodeLiftQueued extends StaticAnnotation

/*
 * Used by compiler plug-in to generate __nclsID static member and getNodeClsID accessor for non-property node
 * generating lambdas when applied to the argument
 */
class withNodeClassID extends StaticAnnotation

/**
 * Used internally by compiler plug-in to verify that tweaks are expected on a given property
 */
@getter
class tweakable extends StaticAnnotation
@getter
class nonTweakable extends StaticAnnotation
class onlyTweakableArg extends StaticAnnotation
// Used internally, and is a separate annotation from @scenarioIndependent so we can distinguish between
// user-specified SI and compiler-inferred SI.
@getter
private[optimus] class scenarioIndependentInternal extends StaticAnnotation

/**
 * Used internally in Optimus code to mark methods which are expecting to receive tweaks or tweakable nodes. In
 * particular:
 *   - when used on a method with @nodeLift, any lifted nodes in the args are checked to ensure they are tweakable
 *   - when used on any method it allows calls to @tweakOperator methods in the arguments
 */
class expectingTweaks extends StaticAnnotation

/**
 * Used internally in Optimus code to mark methods which define a tweak (i.e. := :*= and friends). Such methods can only
 * be called in the argument lists of @expectingTweak methods.
 */
class tweakOperator extends StaticAnnotation

/**
 * Used internally by compiler plugin to tag functions (e.g. def foo$newNode) which are creating the node for a def
 */
class defNodeCreator extends Annotation

/**
 * Used internally by compiler plugin to tag functions (e.g. def foo$newNode) which are creating the node for an async
 * def
 */
class defAsyncCreator extends Annotation

/**
 * Used internally by compiler plugin to tag functions (e.g. def foo$newNode) which are creating the node for a job def
 */
class defJobCreator extends Annotation

/**
 * Used internally by compiler plugin to tag functions (e.g. def foo$newNode) which are creating the node for a val.
 * sadly we have to distinguish this from the def case because the node may need to be rewritten post-typer to support
 * lazy val loading...
 */
class valNodeCreator extends Annotation

/*
 * used to replace <paramaccessor> flag during we transform properties of entity
 */
class valAccessor extends StaticAnnotation

class columnar extends Annotation

class noDalPriql extends Annotation

/*
 * Supports auto-distribution of the annotated method to the grid.
 *
 * @elevated is translated by the compiler to @async(exposeArgTypes = true), with default ElevatedDistributor plugin attached.
 * During runtime, dal env is used to point to appropriate grid (dev/qa/prod)
 *
 * The request will be executed on the grid *using grid's prodID permissions*. User initiating the elevated request must
 * be a member of at least one role listed in the annotation parameter `userRoles`.
 *
 * For audit purposes, any DAL newTransaction block will be marked as if user initiated the DAL write.
 *
 * @param userRoles
 *   - the user needs to be a member of at least one role in order to execute elevated request
 */
class elevated(val userRoles: Set[String]) extends StaticAnnotation

/*
 * _calls_ to a method marked with this annotation are considered to be @scenarioIndependent if and only if any lambda
 * arguments are scenarioIndependent. This annotation can be applied to @node (which are usually considered to be NOT
 * SI, so this annotation slightly relaxes the check) or regular methods (which are usually considered SI, so this
 * annotation increases the checks).
 *
 * This is especially useful for collection traversal methods like 'map' and 'filter' which are SI as long as the lambda
 * is not accessing any non-SI code.
 */
class scenarioIndependentTransparent extends StaticAnnotation

/**
 * tell the plugin this creation method(include apply, uniqueInstance, copyUnique) is generated by optimus plugin,
 * during the optimus_refcheck phase, we won't report 'new' operator error in these auto generated creation methods
 */
class autoGenCreation extends Annotation

// was marked @stored(childToParent=true)
@getter
class c2p extends Annotation

// mark these method we don't want to generate by scalaDoc for user
class excludeDoc extends StaticAnnotation

/**
 * Promise that a method be invoked multiple times in parallel. This could mean that it's referentially transparent, but
 * it doesn't have to be.
 */
private[optimus] class parallelizable extends StaticAnnotation

/**
 * Indicate that a method cannot be executed in parallel.
 */
private[optimus] class sequential extends StaticAnnotation

// Fully suppress checks for async calls in closures used in closure arguments to this function.
private[optimus] class suppressAutoAsync extends StaticAnnotation

// If a method has @assumeParallelizableInClosures, assume that any collection operations
// found in closures in arguments passed to the function are parallelizable and should be
// silently apar'd.
// If a particular parameter has this annotation, assume the above for collection operations
// found in closures in the corresponding argument.
private[optimus] class assumeParallelizableInClosure extends StaticAnnotation

// Allow sync stacks from closures passed *directly* to this function (not desirable)
class closuresEnterGraph extends StaticAnnotation

// Always attempt conversion to corresponding AsyncFunctionN/NodeFunctionN
class alwaysAutoAsyncArgs extends poisoned("@alwaysAutoAsyncArgs function was not transformed")

/**
 * General purpose marker to communicate with later phases
 */
class miscFlags(flags: Int) extends StaticAnnotation

/**
 * Inserts the code location where the annotated method is applied. See [[optimus.platform.LocationTag]].
 */
class withLocationTag extends StaticAnnotation

/**
 * Disallow tweaking this value by property.
 *
 * This is a temporary fix for the bug in OPTIMUS-57047.
 */
class byInstanceOnly extends Annotation

/** Annotated methods cause a compile error at the call site. */
class poisoned(msg: String) extends StaticAnnotation
