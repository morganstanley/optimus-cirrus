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
package optimus.buildtool.resolvers

import coursier.core._
import coursier.util.Xml._
import coursier.util.Xml.Node
import CoursierInterner.interned
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.config.DependencyCoursierKey
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.Exclude

/**
 * Copy of coursier.ivy.IvyXml (which sadly isn't extensible) with some minor changes to support AFS Ivy.
 *
 * TODO (OPTIMUS-26823): Remove the code duplication
 */
private[resolvers] object MsIvyXml {
  protected val log: Logger = getLogger(this)
  val attributesNamespace = "http://ant.apache.org/ivy/extra"

  private def info(node: Node, version: String): Either[String, (Module, String)] =
    for {
      org <- node.attribute("organisation")
      name <- node.attribute("module")
      fixedVersion = node
        .attribute("revision")
        .toOption
        .getOrElse(version) // MS workaround for broken ivys which are addressed in ivy_repo_fixes module.
      attr = node.attributesFromNamespace(attributesNamespace)
    } yield (Module(Organization(org), ModuleName(name), attr.toMap), fixedVersion)

  private def configurations(node: Node): List[(Configuration, List[Configuration])] =
    node.children
      .filter(_.label == "conf")
      .flatMap { node =>
        node.attribute("name").toOption.toSeq.map(_ -> node)
      }
      .map { case (name, node) =>
        Configuration(name) -> (node.attribute("extends").toOption.toList.flatMap(_.split(',')).map(Configuration(_)))
      }
      .toList

  def mappings(mapping: String): Seq[(String, String)] =
    mapping.split(';').toList.flatMap { m =>
      val (froms, tos) = m.split("->", 2) match {
        case Array(from)     => (from, s"default($from)")
        case Array(from, to) => (from, to)
      }

      for {
        from <- froms.split(',').toList
        to <- tos.split(',')
      } yield (from.trim, to.trim)
    }

  private def dependencies(
      node: Node,
      loadedExcludes: Map[DependencyCoursierKey, Seq[Exclude]],
      afsGroupNameToMavenMap: Map[(String, String), Seq[DependencyDefinition]]): Seq[(Configuration, Dependency)] =
    node.children
      .filter(_.label == "dependency")
      .flatMap { node =>
        // artifact and include sub-nodes are ignored here

        val excludes = node.children
          .filter(_.label == "exclude")
          .flatMap { node0 =>
            val org = node0.attribute("org").toOption.getOrElse("*")
            val name = node0.attribute("module").toOption.orElse(node0.attribute("name").toOption).getOrElse("*")
            val confs = node0.attribute("conf").toOption.filter(_.nonEmpty).fold(Seq("*"))(_.split(','))
            confs.map(_ -> (Organization(org), ModuleName(name)))
          }
          .groupBy { case (conf, _) => conf }
          .map { case (conf, l) => conf -> l.map { case (_, e) => e }.toSet }

        val allConfsExcludes = excludes.getOrElse("*", Set.empty)

        for {
          org <- node.attribute("org").toOption.toSeq
          name <- node.attribute("name").toOption.toSeq
          version <- node.attribute("rev").toOption.toSeq
          rawConf <- node.attribute("conf").toOption.toSeq
          (fromConf, toConf) <- mappings(rawConf)
          attr = node.attributesFromNamespace(attributesNamespace)
        } yield {
          val coursierExcludes = allConfsExcludes ++ excludes.getOrElse(fromConf, Set.empty)
          val obtExcludes = loadedExcludes
            .getOrElse(DependencyCoursierKey(org, name, fromConf, version), Nil)
            .map { e =>
              (Organization(e.group.getOrElse("*")), ModuleName(e.name.getOrElse("*")))
            }
            .toSet
          val transitive = node.attribute("transitive").toOption match {
            case Some("false") => false
            case _             => true
          }
          val unmappedModule = Module(Organization(org), ModuleName(name), attr.toMap)
          // try transitive maven dependency mapping
          val mappedMavenDeps: Seq[(Module, String)] =
            MavenUtils.applyTransitiveMapping(org, name, attr, afsGroupNameToMavenMap)
          val resolvedDeps =
            if (mappedMavenDeps.nonEmpty) mappedMavenDeps else Seq((unmappedModule, version))
          resolvedDeps.map { case (mappedModule, mappedVersion) =>
            Configuration(fromConf) -> interned(
              Dependency(
                mappedModule,
                mappedVersion,
                Configuration(toConf),
                coursierExcludes ++ obtExcludes,
                Attributes(
                  Type(mappedModule.attributes.getOrElse("type", "")),
                  Classifier(mappedModule.attributes.getOrElse("classifier", ""))),
                optional = false,
                transitive = transitive
              ))
          }
        }
      }
      .flatten

  private def publications(node: Node): Map[Configuration, Seq[Publication]] =
    node.children
      .filter(_.label == "artifact")
      .flatMap { node =>
        val name = node.attribute("name").toOption.getOrElse("")
        val type0 = node.attribute("type").toOption.getOrElse("jar")
        val ext = node.attribute("ext").toOption.getOrElse(type0)
        val confs = node.attribute("conf").toOption.fold(Seq("*"))(_.split(','))
        val classifier = node.attribute("classifier").toOption.getOrElse("")
        confs.map(_ -> Publication(name, Type(type0), Extension(ext), Classifier(classifier)))
      }
      .groupBy { case (conf, _) => conf }
      .map { case (conf, l) => Configuration(conf) -> l.map { case (_, p) => p } }

  def project(
      node: Node,
      version: String,
      excludes: Map[DependencyCoursierKey, Seq[Exclude]],
      afsGroupNameToMavenMap: Map[(String, String), Seq[DependencyDefinition]]): Either[String, Project] =
    for {
      infoNode <- node.children
        .find(_.label == "info")
        .toRight("Info not found")

      modVer <- info(infoNode, version)
    } yield {

      val (module, version) = modVer

      val dependenciesNodeOpt = node.children
        .find(_.label == "dependencies")

      val dependencies0 =
        dependenciesNodeOpt.map(dependencies(_, excludes, afsGroupNameToMavenMap)).getOrElse(Nil)

      val dependenciesByConf = dependencies0.groupBy(_._1)

      val configurationsNodeOpt = node.children
        .find(_.label == "configurations")

      val configurationsOpt = configurationsNodeOpt.map(configurations)

      val configurations0 = configurationsOpt.getOrElse(List(Configuration("default") -> List.empty[Configuration]))

      val configurations0Map = configurations0.toMap

      val publicationsNodeOpt = node.children
        .find(_.label == "publications")

      val publicationsOpt = publicationsNodeOpt.map(publications)

      val description = infoNode.children
        .find(_.label == "description")
        .map(_.textContent.trim)
        .getOrElse("")

      val licenses = infoNode.children
        .filter(_.label == "license")
        .flatMap { n =>
          n.attribute("name")
            .toOption
            .map { name =>
              (name, n.attribute("url").toOption)
            }
            .toSeq
        }

      val publicationDate = infoNode
        .attribute("publication")
        .toOption
        .flatMap(parseDateTime)

      // checking unused excludes
      dependenciesByConf.foreach { case (conf, deps) =>
        val transitiveConfigsDepsList: Seq[Dependency] =
          configurations0Map.getOrElse(conf, List.empty).flatMap(dependenciesByConf.getOrElse(_, Seq.empty).map(_._2))
        val allDeps: Seq[Dependency] = deps.map(_._2) ++ transitiveConfigsDepsList
        val depsOrgNameList: Seq[(String, String)] = allDeps.map { d =>
          (d.module.organization.value, d.module.name.value)
        }
        val key = DependencyCoursierKey(module.organization.value, module.name.value, conf.value, version)
        excludes.getOrElse(key, Seq.empty).foreach { e =>
          val excludeOrg = e.group.getOrElse("")
          val excludeName = e.name.getOrElse("")
          if (!depsOrgNameList.contains((excludeOrg, excludeName)))
            log.warn(s"[${key.displayName}] Unused excludes '$excludeOrg:$excludeName' detected!")
        }
      }

      Project(
        module,
        version,
        dependencies0.toVector,
        configurations0Map,
        None,
        Nil,
        Nil,
        Nil,
        None,
        None,
        None,
        false,
        None,
        if (publicationsOpt.isEmpty)
          // N.B. Coursier supplies a default artifact if none is specified, but that seems to create more problems than
          // it solves for us in MS (because that default artifact generally doesn't match exist so we get errors),
          // so we'll return Nil for now
          Nil
        else {
          // publications node is there -> only its content (if it is empty, no artifacts,
          // as per the Ivy manual)
          val inAllConfs = publicationsOpt.flatMap(_.get(Configuration.all)).getOrElse(Nil)
          configurations0.flatMap { case (conf, _) =>
            (publicationsOpt.flatMap(_.get(conf)).getOrElse(Nil) ++ inAllConfs).map(conf -> _)
          }.toVector
        },
        Info(
          description,
          "",
          licenses,
          Nil,
          publicationDate,
          None
        )
      )
    }

}
