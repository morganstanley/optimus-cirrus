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
package optimus.observability

import optimus.entity.EntityInfoRegistry
import optimus.graph.DefPropertyInfo0
import optimus.graph.GraphException
import optimus.graph.NodeTaskInfo
import optimus.platform._
import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.NodeInputMapValue
import optimus.platform.inputs.NodeInputResolver
import optimus.platform.inputs.dist.DistNodeInputs
import optimus.platform.inputs.dist.GSFSections
import optimus.platform.inputs.dist.GSFSections.EngineSpecEnvVar
import optimus.platform.inputs.dist.GSFSections.EngineSpecJavaOpt
import optimus.platform.inputs.loaders.FoldedNodeInput
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.registry.Registry
import optimus.platform.inputs.registry.Source
import optimus.platform.storable.Entity

/** An input to the @job computation */
@embeddable
sealed trait JobInput

/**
 * A tweak input to the @job computation. Capture is supported for [[Tweak]] targeting a parameter-less @node on an
 * entity object.
 *
 * @see
 *   [[JobInput]]
 */
@embeddable
final case class TweakJobInput(propertyInfoName: String, className: String, value: JobValue) extends JobInput {

  /** @return the corresponding [[Tweak]] associated to this [[TweakJobInput]] */
  private[observability] def asTweak: Tweak = {
    // tweak can be easily be recreated by finding the object entity
    val klass = Class.forName(className)
    val entity = EntityInfoRegistry.getModule(klass)
    // using the node key constructor on the property info (safe cast because target @node is parameter-less)
    val propertyInfo = entity.$info.propertyMetadata(propertyInfoName).asInstanceOf[DefPropertyInfo0[Entity, _, _, _]]
    val nodeKey = propertyInfo.createNodeKey(entity)
    // creating an instance property target tweak on the node key with the desired value
    SimpleValueTweak(nodeKey)(value.value)
  }
}

object TweakJobInput {
  def apply(propertyInfo: NodeTaskInfo, value: AnyRef): TweakJobInput =
    TweakJobInput(
      propertyInfo.name(),
      propertyInfo.entityInfo.runtimeClass.getName,
      JobValue.toJobValue(value, debugValue = propertyInfo))
}

/**
 * A scenario-independent [[NodeInput]] to the @job computation.
 *
 * @see
 *   [[JobInput]]
 */
@embeddable
sealed trait SIJobInput extends JobInput {

  /** @see [[NodeInput.name()]] */
  def name: String

  /** @see [[NodeInput.description()]] */
  def description: String

  /** @return the [[JobValue]] bound to this [[JobInput]] */
  def value: JobValue

  /** Overrides the bound [[JobValue]] for this [[JobInput]] */
  def withValue(newValue: JobValue): SIJobInput

  def isLocal: Boolean
  def forwardToEngine: Boolean
  private[optimus] def affectsExecutionProcessWide: Boolean
  private[optimus] def requiresRestart: Boolean = false
  private[optimus] def asNodeInput: NodeInputResolver.Entry[Any]
}

object SIJobInput {

  def apply(foldedNodeInput: FoldedNodeInput): SIJobInput = foldedNodeInput.nodeInput match {
    case nodeInput if nodeInput.gsfSection.requiresRestart() =>
      // if it requires restart, handle it separately because we need to be able to
      // create an engine spec in a code-version independent manner
      val gsfSection = nodeInput.gsfSection()
      gsfSection match {
        case jo: EngineSpecJavaOpt[AnyRef] =>
          JavaOptJobInput(foldedNodeInput, jo)
        case ev: EngineSpecEnvVar[AnyRef] =>
          EnvVarJobInput(foldedNodeInput, ev)
      }
    case p: PluginTagKey[AnyRef] => PluginTagJobInput(p, foldedNodeInput.value)
    case nodeInput if Registry.TheHugeRegistryOfAllOptimusProperties.contains(nodeInput.name) =>
      RegistryJobInput(foldedNodeInput)
    case _ =>
      throw new GraphException(s"Unexpected matched value: $foldedNodeInput")

  }

  def unapply(siJobInput: SIJobInput): Option[(String, JobValue)] = Some(siJobInput.name, siJobInput.value)
}

@embeddable
final case class PluginTagJobInput(className: String, name: String, description: String, value: JobValue)
    extends SIJobInput {
  private[observability] def asPluginTagKeyValue: PluginTagKeyValue[Any] = {
    // lookup the plugin tag via class reflection
    val klass = Class.forName(className)
    val pluginTagKey = klass.getField("MODULE$").get(klass).asInstanceOf[PluginTagKey[Any]]
    PluginTagKeyValue(pluginTagKey, value.value)
  }
  override def withValue(newValue: JobValue): PluginTagJobInput = copy(value = newValue)
  override def isLocal = true
  override def forwardToEngine = true
  private[optimus] override def affectsExecutionProcessWide = false
  private[optimus] override def asNodeInput: NodeInputResolver.Entry[Any] = asPluginTagKeyValue
}

object PluginTagJobInput {
  def apply(pluginTagKey: PluginTagKey[AnyRef], value: AnyRef): PluginTagJobInput =
    PluginTagJobInput(
      pluginTagKey.getClass.getName,
      pluginTagKey.name,
      pluginTagKey.description,
      JobValue.toJobValue(value, debugValue = pluginTagKey))
}

@embeddable
final case class JavaOptJobInput(
    nameOpt: Option[String],
    description: String,
    javaProperty: Option[String],
    pattern: String,
    javaValue: String,
    isLocal: Boolean,
    forwardToEngine: Boolean,
    private[optimus] val affectsExecutionProcessWide: Boolean)
    extends SIJobInput {
  def name: String = nameOpt.getOrElse(asJavaOpt)
  override def value: JobValue = StrJobValue(javaValue)
  def asJavaOpt: String = GSFSections.EngineSpecJavaOpt.format(javaProperty.orNull, pattern, javaValue)
  override def withValue(newValue: JobValue): JavaOptJobInput = newValue match {
    case StrJobValue(v) => copy(javaValue = v)
    case _              => throw new GraphException(s"Incompatible type $newValue, string is required")
  }
  override def requiresRestart: Boolean = true
  private[optimus] override def asNodeInput: NodeInputResolver.Entry[Any] = {
    // could be a jvm property or a java property, so look both up
    val javaEntry = Registry.TheHugeRegistryOfAllOptimusProperties.lookup(Source.JavaProperty.key(javaProperty.orNull))
    val entry =
      if (javaEntry.isPresent) {
        javaEntry.get.asNodeInputWithValue(javaValue, LoaderSource.SYSTEM_PROPERTIES)
      } else {
        // otherwise fall back to regular raw java opt
        val nodeInput = DistNodeInputs.newOptimusDistJavaFlag(asJavaOpt)
        NodeInputResolver
          .entry(nodeInput, new NodeInputMapValue[String](LoaderSource.SYSTEM_PROPERTIES, asJavaOpt))
      }
    entry.asInstanceOf[NodeInputResolver.Entry[Any]]
  }
}

object JavaOptJobInput {
  def apply(foldedNodeInput: FoldedNodeInput, jo: EngineSpecJavaOpt[AnyRef]): SIJobInput = {
    val value = jo.invert(foldedNodeInput.value)
    // if this node input cannot be found in the registry, then it's generated
    val name =
      Option(Registry.TheHugeRegistryOfAllOptimusProperties.lookupByName(foldedNodeInput.nodeInput.name))
        .map(_.name)
    JavaOptJobInput(
      name,
      foldedNodeInput.nodeInput.description,
      Option(jo.javaProperty()),
      jo.pattern(),
      jo.invert(foldedNodeInput.value),
      foldedNodeInput.isLocal,
      foldedNodeInput.forwardToEngine,
      foldedNodeInput.nodeInput.affectsExecutionProcessWide
    )
  }
}

@embeddable
final case class EnvVarJobInput(
    name: String,
    description: String,
    envKey: String,
    envValue: String,
    isLocal: Boolean,
    forwardToEngine: Boolean,
    private[optimus] val affectsExecutionProcessWide: Boolean)
    extends SIJobInput {
  override def value: JobValue = StrJobValue(envValue)
  override def withValue(newValue: JobValue): EnvVarJobInput = newValue match {
    case StrJobValue(v) => copy(envValue = v)
    case _              => throw new GraphException(s"Incompatible type $newValue, string is required")
  }
  override def requiresRestart: Boolean = true
  private[optimus] override def asNodeInput: NodeInputResolver.Entry[Any] = {
    // look it up if present in the registry
    val envVarEntry = Registry.TheHugeRegistryOfAllOptimusProperties.lookup(Source.EnvironmentVariable.key(envKey))
    val entry =
      if (envVarEntry.isPresent) {
        envVarEntry.get.asNodeInputWithValue(envValue, LoaderSource.ENVIRONMENT_VARIABLES)
      } else {
        // otherwise fall-back to regular raw environment variable
        val nodeInput = DistNodeInputs.newOptimusDistEnvironmentVariable(envKey)
        NodeInputResolver
          .entry(nodeInput, new NodeInputMapValue[String](LoaderSource.ENVIRONMENT_VARIABLES, envValue))
      }
    entry.asInstanceOf[NodeInputResolver.Entry[Any]]
  }
}

object EnvVarJobInput {
  def apply(foldedNodeInput: FoldedNodeInput, ev: EngineSpecEnvVar[AnyRef]): SIJobInput = {
    val entry = ev.invertAndFormat(foldedNodeInput.value)
    EnvVarJobInput(
      foldedNodeInput.nodeInput.name,
      foldedNodeInput.nodeInput.description,
      entry.getKey,
      entry.getValue,
      foldedNodeInput.isLocal,
      foldedNodeInput.forwardToEngine,
      foldedNodeInput.nodeInput.affectsExecutionProcessWide
    )
  }
}

@embeddable
final case class RegistryJobInput(
    name: String,
    description: String,
    value: JobValue,
    isLocal: Boolean,
    forwardToEngine: Boolean,
    private[optimus] val affectsExecutionProcessWide: Boolean,
    private[optimus] val loaderSource: String)
    extends SIJobInput {
  override def withValue(newValue: JobValue): RegistryJobInput = copy(value = newValue)
  private[optimus] override def asNodeInput: NodeInputResolver.Entry[Any] = {
    val nodeInput = Registry.TheHugeRegistryOfAllOptimusProperties.lookupByName(name)
    if (nodeInput eq null) {
      throw new IllegalStateException(s"Node input not found (name: $name)")
    }
    NodeInputResolver.entry(
      nodeInput.asInstanceOf[NodeInput[Any]],
      new NodeInputMapValue(LoaderSource.valueOf(loaderSource), value.value))
  }
}

object RegistryJobInput {
  def apply(foldedNodeInput: FoldedNodeInput): RegistryJobInput = RegistryJobInput(
    foldedNodeInput.nodeInput.name,
    foldedNodeInput.nodeInput.description,
    JobValue.toJobValue(foldedNodeInput.value, debugValue = foldedNodeInput.nodeInput),
    foldedNodeInput.isLocal,
    foldedNodeInput.forwardToEngine,
    foldedNodeInput.nodeInput.affectsExecutionProcessWide,
    foldedNodeInput.source.toString
  )
}
