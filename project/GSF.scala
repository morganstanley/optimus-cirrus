package optimus

import optimus.Dependencies._
import sbt._
import sbt.Keys._

object GSF {
  private val projectsDir = file("optimus/gsf/projects")
  
  val breadcrumbs = Project("gsfBreadcrumbs", projectsDir / "gsf_breadcrumbs")
    .settings(libraryDependencies ++= Seq(scalaCollectionCompat, sprayJson))
    .dependsOn(Platform.breadcrumbs)
}
