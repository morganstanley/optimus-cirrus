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
package optimus.breadcrumbs.zookeeper

import java.io.Closeable
import java.util.{List => JavaList, ArrayList => JavaArrayList, Map => JavaMap, HashMap => JavaHashMap}

import msjava.zkapi.PropertySource
import msjava.zkapi.ZkaConfig
import msjava.zkapi.internal.ZkaContext
import msjava.zkapi.internal.ZkaData
import msjava.zkapi.internal.ZkaPropertySource
import optimus.breadcrumbs._
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.filter.CrumbFilter
import optimus.breadcrumbs.routing.CrumbRoutingRule
import optimus.utils.PropertyUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal
import optimus.scalacompat.collection._

object BreadcrumbsPropertyConfigurer {

  private val log = LoggerFactory.getLogger("BreadcrumbsPropertyConfigurer")
  private val closeablePropertySources, closeableContexts = mutable.Set.empty[Closeable]
  private val closeables = List(closeablePropertySources, closeableContexts).view

  private val FilterKey: String = "filters"
  private val RuleKey: String = "rules"
  private val KafkaPublisherKey: String = "kafka"
  private val LogPublisherKey: String = "log"
  private val NoPublisherKey: String = "off"
  private val MergeKey: String = "_merge"

  protected[breadcrumbs] val KafkaParamsKey: String = "kafkaparams"
  protected[breadcrumbs] val FilterParamsKey: String = "filterparams"
  protected[breadcrumbs] val TopicParamsKey: String = "topicparams"
  protected[breadcrumbs] val PublisherKey: String = "publisher"
  private val RoutingKey: String = "routing"

  private val configPropertyRoot = "breadcrumb.config"
  private val aliasingEnabled = System.getProperty(s"$configPropertyRoot.aliasing.enabled", "false").toBoolean
  private val maxAliasingLevels = System.getProperty(s"$configPropertyRoot.aliasing.maxdepth", "3").toInt
  private val maxMergeDepth = System.getProperty(s"$configPropertyRoot.merge.maxdepth", "3").toInt
  private val useFlatConfig = System.getProperty(s"$configPropertyRoot.flat", "true").toBoolean

  /*
  Example of how we could listen for updates.
  if(log.is_Debug_Enabled) {  // actually you need to remove the underscores.
    zk.addUpdateListener(new WatchedUpdateListener {
      override def updateConfiguration(result: WatchedUpdateResult): Unit = {
        log.debug(s"added=${ result.getAdded }, changed=${ result.getChanged }}, completed=${ result.getComplete }, deleted=${ result.getDeleted }")
      }
    })
  }
   */

  def findCase[A](ps: PropertySource, property: String, customizationKeys: Map[String, String]): Option[A] = {
    val data = ps.getProperty(property)
    if (data eq null) return None
    val cases = data.asInstanceOf[JavaMap[String, JavaList[JavaMap[String, AnyRef]]]].get("cases")
    if (cases eq null) return None
    val configs: Seq[Map[String, AnyRef]] = cases.asScala.map(_.asScala.toMap)
    log.debug(s"Checking customization keys $customizationKeys against zk cases")
    configs
      .find { zkCase: Map[String, AnyRef] =>
        {
          log.debug(s"Checking against case $zkCase")
          // Every zk key should be present in the customization map and match.
          zkCase.forall {
            case (k, v) =>
              (k == property) || (k == "_default") ||
                customizationKeys.get(k).exists(_.matches(v.toString))
          }
        }
      }
      .map { c: Map[String, AnyRef] =>
        val v = c(property)
        log.info(s"Match $customizationKeys ~ $c")
        v.asInstanceOf[A]
      }
  }

  def getBCConfig(ps: PropertySource, keys: Map[String, String]): Option[BreadcrumbConfig] = {
    findCase[JavaMap[String, Any]](ps, "bcparams", keys)
      .map(_.asScala.toMap.mapValuesNow(_.toString))
      .map(new BreadcrumbConfigFromMap(_))
  }

  /** Try to find an appropriate property source given a zk path (implicitly preceded by "breadcrumbs/".)
   * If there's nothing at all at the path, we return none.
   * If there's an "alias" attribute of type String, we recurse to evaluate
   * it as a possible path.
   */
  private[this] def pathToPS(
      czkc: CachedZkaContext,
      path: String,
      nAliasRecursion: Int = maxAliasingLevels): Option[PropertySource] = {
    val p = if (path.length > 1) s"/breadcrumbs/$path" else "/breadcrumbs"
    log.info(s"Looking for configuration in $p")

    lazy val ctx = {
      val result = ZkaContext.contextForSubPath(czkc.context, p)
      log.debug(s"initialized ZK context for property source at $p")
      closeableContexts += result
      result
    }

    if (nAliasRecursion <= 0) {
      log.warn("Too many levels of alias recursion")
      None
    } else {
      Try { czkc.getData(p) }.toOption flatMap { nodeData =>
        // There's something in this context.  Either a value or a property source.
        val alias = if (aliasingEnabled) nodeData.get("alias") else 0x4DC109DC
        alias match {
          case a: String if a.nonEmpty =>
            log.info(s"Following alias $a")
            pathToPS(czkc, a, nAliasRecursion - 1)
          case _ =>
            val ps =
              if (useFlatConfig)
                new ReadOnlyYamlPropertySource(YamlContext(nodeData.getRawMap().asInstanceOf[JavaMap[String, Object]]))
              else
                new ZkaPropertySource("", ctx)
            closeablePropertySources += ps
            Some(ps)
        }
      } orElse {
        // Nothing here at all, not even an alias.
        log.info(s"Didn't find any configuration at $p")
        None
      }
    }
  }

  private[this] def findFilterConfigs(
      czkc: CachedZkaContext,
      ps: PropertySource,
      keys: Map[String, String]): Option[Map[String, Seq[JavaMap[String, AnyRef]]]] = {
    findParamsRecursivelyEx(czkc, ps, keys, FilterParamsKey, FilterKey).flatMap { config =>
      Option(config.get(FilterKey)).map {
        // as consistent with other cases, for any breadcrumbs config related ZK node, the data has to be json
        // object convertible if they ever exist. (i.e., the cast will fail if the node exists but empty)
        _.asInstanceOf[JavaList[JavaMap[String, Object]]].asScala
        // every valid filter should have a 'publisher' (kafka or log, for example) configured
          .filter(_.asScala.get(PublisherKey).isDefined)
          .groupBy(_.get(PublisherKey).toString)
      }
    }
  }

  /**
   * Find kafka parameter map by from the "kafkaparams" or "topicparams" property of the specified property source.
   * Treat "_merge" in the parameter map specially: it is taken to specify the path of another property source,
   * from which to extract a base map, into which to merge.
   */
  private[this] def findParamsRecursivelyEx(
    czkc: CachedZkaContext,
    ps: PropertySource,
    keys: Map[String, String],
    property: String,
    nestedArrayProperty: String = "",
    nMaxRecursion: Int = maxMergeDepth): Option[JavaMap[String, AnyRef]] = {
    if (nMaxRecursion <= 0)
      None
    else
      findCase[JavaMap[String, Object]](ps, property, keys).map { props: JavaMap[String, Object] =>
        Option(props.get(MergeKey))
          .flatMap { mergeValue =>
            pathToPS(czkc, mergeValue.toString)
          }
          .flatMap { ps =>
            findParamsRecursivelyEx(czkc, ps, keys, property, nestedArrayProperty, nMaxRecursion - 1)
          }
          .fold {
            // Nothing to merge with
            props
          } { defaults =>
            // Merge current properties into what we found at mergeKey, except the merge key itself.
            props.remove(MergeKey)
            if (nestedArrayProperty.nonEmpty) {
              // The merge logic is slightly different for nested array properties
              val mergedProps = new JavaArrayList[JavaMap[String, Object]]
              Option(props.get(nestedArrayProperty)).map(_.asInstanceOf[JavaList[JavaMap[String, Object]]]).foreach {
                mergedProps.addAll(_)
              }
              Option(defaults.get(nestedArrayProperty)).map(_.asInstanceOf[JavaList[JavaMap[String, Object]]]).foreach {
                mergedProps.addAll(_)
              }
              if (!mergedProps.isEmpty)
                defaults.put(nestedArrayProperty, mergedProps)
            } else {
              defaults.putAll(props)
            }
            defaults
          }
      }
  }

  private[this] def ignorer(czkc: CachedZkaContext, keys: Map[String, String]): BreadcrumbsPublisher = {
    log.info("Breadcrumbs ignorer configured")
    new BreadcrumbsIgnorer() {
      override val savedCustomization = Some((keys, czkc.context))
      override def toString = classOf[BreadcrumbsIgnorer].getSimpleName
    }
  }

  private[breadcrumbs] def implFromConfig(
      zkc: ZkaContext,
      appKeys: Map[String, String] = Map()): BreadcrumbsPublisher = {

    CachedZkaContext.withCache(zkc) { czkc =>
      /**
       * Try to find an appropriate property source given a configuration map.  In order, try:
       * 1. An explicit "breadcrumb.config" or "breadcrumb.config.location" element.  Treat its value as the subpath (after "/breadcrumbs/")
       * 2. A runtime environment from "rtcEncv", "rtcMode" or zkMode", e.g. "dev".  Treat its value as the subpath.
       * 3. An "optimus.dsi.uri".  Treat everything after the "//" as the subpath.
       * 4. Try no subpath, i.e. "/breadcrumbs" directly.
       * In all cases, the path may resolve to an alias, which pathToPS will resolve.
       */
      def findPS(appKeys: Map[String, String]): Option[PropertySource] = {
        try {
          PropertyUtils.get("breadcrumb.config.location").flatMap(pathToPS(czkc, _)) orElse
            appKeys.get("breadcrumb.config").flatMap(pathToPS(czkc, _)) orElse
            appKeys.get("rtcEnv").flatMap(pathToPS(czkc, _)) orElse
            appKeys.get("rtcMode").flatMap(pathToPS(czkc, _)) orElse
            appKeys.get("zkMode").flatMap(pathToPS(czkc, _)) orElse
            // The uri could take the forms
            //     foo://bar/wiz/env
            //     foo://bar/wiz?blip=env
            // where the actual environment is env.
            appKeys
              .get("optimus.dsi.uri")
              .map(_.replaceFirst(".*:\\/\\/(.*\\?\\w+=)?", ""))
              .flatMap(pathToPS(czkc, _)) orElse
            pathToPS(czkc, "").map { ps =>
              closeablePropertySources += ps
              ps
            }
        } catch {
          case NonFatal(ex) => {
            log.warn(s"Caught exception while creating property source, possibly due to invalid configuration data", ex)
            None
          }
        }
      }

      val SYS = "(SYS_\\w+)".r
      val locspec: Map[String, String] = System
        .getenv()
        .asScala
        .collect { case (SYS(k), v) => k -> v }
        .toMap
      val keys = appKeys ++ locspec ++ PropertyUtils.get("breadcrumb.special.code").map("specialCode" -> _).toList

      val maybePropertySource = findPS(keys)

      val ignorer: BreadcrumbsPublisher = new BreadcrumbsIgnorer() {
        override val savedCustomization = Some((keys, czkc.context))
        override def init(): Unit = log.info("Initialized crumb ignorer")
        override def toString = classOf[BreadcrumbsIgnorer].getSimpleName
      }

      lazy val kafkaPublisher = safeResolvePublisher(KafkaPublisherKey)
      lazy val logPublisher = safeResolvePublisher(LogPublisherKey)

      def safeResolvePublisher(publisher: String) = {
        try {
          maybePropertySource
            .map { ps =>
              resolvePublisher(czkc, ps, publisher, keys)
            }
            .getOrElse(throw new RuntimeException(s"$publisher failed to initialize due to missing property source"))
        } catch {
          case NonFatal(ex) => {
            log.warn(s"Caught exception $ex configuring $publisher; reverting to crumb ignorer")
            log.debug("Exception:", ex)
            ignorer
          }
        }
      }

      def select(publisher: String): BreadcrumbsPublisher = {
        val publishers = publisher.split(",")
        if (publishers.length > 1)
          new BreadcrumbsCompositePublisher(publishers.toSet map select)
        else publisher match {
          case KafkaPublisherKey => kafkaPublisher
          case LogPublisherKey   => logPublisher
          case NoPublisherKey => {
            log.info("Publishing turned off, selecting crumb ignorer")
            ignorer
          }
          case x => {
            log.info(s"Unknown breadcrumbs resource '$x', selecting crumb ignorer")
            ignorer
          }
        }
      }

      val configuredPublisher = try {
        (for (ps <- maybePropertySource;
              publisher <- findCase[String](ps, PublisherKey, keys))
          yield select(publisher)) getOrElse ignorer
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Caught exception $ex configuring breadcrumbs; reverting to crumb ignorer")
          log.debug("Exception:", ex)
          ignorer
      }

      val routingConfiguration = try {
        (for (ps <- maybePropertySource;
              routing <- findParamsRecursivelyEx(czkc, ps, keys, RoutingKey, RuleKey))
          yield {
            Option(routing.get(RuleKey))
              .map { _.asInstanceOf[JavaList[JavaMap[String, Object]]].asScala }
              .getOrElse(Seq.empty)
          }).getOrElse(Seq.empty)
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Caught exception $ex setting up breadcrumbs routing")
          log.debug("Exception:", ex)
          Seq.empty
      }

      val rules = try {
        routingConfiguration.map { r =>
          val filters = r.get(FilterKey).asInstanceOf[JavaList[JavaMap[String, Object]]].asScala
          CrumbFilter.createFilter(filters.map(_.asScala.toMap)) map { filter =>
            val name = r.getOrDefault("name", "N/A").toString()
            val publisher = select(Option(r.get(PublisherKey)).map(_.toString).getOrElse(NoPublisherKey))
            CrumbRoutingRule(name, filter, publisher)
          }
        } collect { case Some(routingRule) => routingRule }
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Caught exception $ex setting up breadcrumbs routing, no rules will be applied")
          log.debug("Exception:", ex)
          Seq.empty
      }

      new BreadcrumbsRouter(rules, configuredPublisher)
    }
  }

  private[breadcrumbs] def resolvePublisher(
      czkc: CachedZkaContext,
      ps: PropertySource,
      publisher: String,
      keys: Map[String, String]) = {

    val zkc = czkc.context
    publisher match {
      case KafkaPublisherKey =>
        val topicParams: JavaMap[String, Object] = findParamsRecursivelyEx(czkc, ps, keys, TopicParamsKey)
          .getOrElse(new JavaHashMap[String, Object]())
        findParamsRecursivelyEx(czkc, ps, keys, KafkaParamsKey).fold(ignorer(czkc, keys)) { props =>
          findFilterConfigs(czkc, ps, keys) flatMap { filterConfigs =>
            filterConfigs.get(KafkaPublisherKey).map { configs =>
              val crumbFilter = CrumbFilter.createFilter(configs.map(_.asScala.toMap))
              new BreadcrumbsKafkaPublisher(props, topicParams) {
                override val savedCustomization: Option[(Map[String, String], ZkaContext)] = Some((keys, zkc))
                override def getFilter: Option[CrumbFilter] = crumbFilter
                override def toString = classOf[BreadcrumbsKafkaPublisher].getSimpleName
              }
            }
          } getOrElse new BreadcrumbsKafkaPublisher(props, topicParams) {
            override val savedCustomization = Some((keys, zkc))
            override def toString = s"${classOf[BreadcrumbsKafkaPublisher].getSimpleName}*"
          }
        }

      case LogPublisherKey =>
        getBCConfig(ps, keys).fold(ignorer(czkc, keys)) { conf =>
          findFilterConfigs(czkc, ps, keys) flatMap { filterConfigs =>
            filterConfigs.get(LogPublisherKey).map { configs =>
              val crumbFilter = CrumbFilter.createFilter(configs.map(_.asScala.toMap))
              new BreadcrumbsLoggingPublisher(conf) {
                override val savedCustomization: Option[(Map[String, String], ZkaContext)] = Some((keys, zkc))
                override def getFilter: Option[CrumbFilter] = crumbFilter
                override def toString = classOf[BreadcrumbsLoggingPublisher].getSimpleName
              }
            }
          } getOrElse new BreadcrumbsLoggingPublisher(conf) {
            override val savedCustomization = Some((keys, zkc))
            override def toString = s"${ classOf[BreadcrumbsLoggingPublisher].getSimpleName }*"
          }
        }

      case x =>
        log.info(s"Unknown breadcrumbs resource $x")
        ignorer(czkc, keys)
    }
  }

}

protected[breadcrumbs] class CachedZkaContext(val context: ZkaContext) {
  import CachedZkaContext._

  private val cache: mutable.Map[String, ZkaData] = mutable.Map.empty
  private var size = 0

  def getData(childPath: String) = {
    if (size >= maxCacheSize) {
      log.debug("cache size exceeded, will resolve request from Zookeeper")
      context.getData(childPath)
    } else
      cache.getOrElseUpdate(childPath, {
        log.debug("cache miss")
        size += 1
        context.getData(childPath)
      })
  }
  def clear() = cache.clear()
}

protected[breadcrumbs] object CachedZkaContext {
  private val log = LoggerFactory.getLogger(classOf[CachedZkaContext])

  private val maxCacheSize = 32 // just a tiny little cache to help during a single init thrust

  def withCache[T](ctx: ZkaContext)(f: CachedZkaContext => T): T = ctx.synchronized {
    val cache = new CachedZkaContext(ctx)
    try {
      f(cache)
    } finally {
      cache.clear
    }
  }
}
