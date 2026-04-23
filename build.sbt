ThisBuild / tlBaseVersion       := "0.1"
ThisBuild / organization        := "com.ruiandrebatista"
ThisBuild / organizationName    := "Rui Batista"
ThisBuild / licenses            := Seq(License.Apache2)
ThisBuild / headerCreate / skip := true
ThisBuild / headerCheck / skip  := true
ThisBuild / tlCiHeaderCheck     := false
ThisBuild / developers          := List(
  tlGitHubDev("ragb", "Rui Batista")
)

ThisBuild / scalaVersion := "3.8.3"

ThisBuild / tlJdkRelease    := Some(17)
ThisBuild / tlFatalWarnings := true

val skunkV           = "1.0.0"
val skunkCirceV      = "1.0.0"
val circeV           = "0.14.15"
val catsEffectV      = "3.7.0"
val munitV           = "1.2.4"
val munitCatsEffectV = "2.1.0"
val ironV            = "3.0.2"
val refinedV         = "0.11.3"
val testcontainersV  = "0.44.1"
val dumboV           = "0.9.0"
val otel4sV          = "0.16.0"
val tapirV           = "1.11.9"
val http4sV          = "0.23.30"
val cirisV           = "3.6.0"
val ducktapeV        = "0.2.12"

lazy val root = tlCrossRootProject.aggregate(core, iron, refined, circe, tests, example, docs)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    name := "skunk-sharp-core",
    libraryDependencies ++= Seq(
      "org.tpolecat"  %% "skunk-core"        % skunkV,
      "org.typelevel" %% "cats-effect"       % catsEffectV,
      "org.scalameta" %% "munit"             % munitV           % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectV % Test
    )
  )

lazy val iron = project
  .in(file("modules/iron"))
  .dependsOn(core)
  .settings(
    name := "skunk-sharp-iron",
    libraryDependencies ++= Seq(
      "io.github.iltotore" %% "iron"              % ironV,
      "org.scalameta"      %% "munit"             % munitV           % Test,
      "org.typelevel"      %% "munit-cats-effect" % munitCatsEffectV % Test
    )
  )

lazy val refined = project
  .in(file("modules/refined"))
  .dependsOn(core)
  .settings(
    name := "skunk-sharp-refined",
    libraryDependencies ++= Seq(
      "eu.timepit"    %% "refined"           % refinedV,
      "org.scalameta" %% "munit"             % munitV           % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectV % Test
    )
  )

lazy val circe = project
  .in(file("modules/circe"))
  .dependsOn(core)
  .settings(
    name := "skunk-sharp-circe",
    libraryDependencies ++= Seq(
      "org.tpolecat"  %% "skunk-circe"       % skunkCirceV,
      "io.circe"      %% "circe-core"        % circeV,
      "org.scalameta" %% "munit"             % munitV           % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectV % Test
    )
  )

lazy val example = project
  .in(file("modules/example"))
  .dependsOn(core, circe)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "skunk-sharp-example",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"     % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe"        % tapirV,
      "org.http4s"                  %% "http4s-ember-server"     % http4sV,
      "is.cir"                      %% "ciris"                   % cirisV,
      "io.github.arainko"           %% "ducktape"                % ducktapeV,
      "dev.rolang"                  %% "dumbo"                   % dumboV,
      "org.typelevel"               %% "otel4s-core"             % otel4sV
    ),
    Compile / unmanagedClasspath += (Compile / resourceDirectory).value
  )

lazy val docs = project
  .in(file("docs"))
  .dependsOn(core, iron, circe)
  .enablePlugins(TypelevelSitePlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name          := "skunk-sharp-docs",
    mdocIn        := baseDirectory.value / "docs",
    mdocVariables := Map("VERSION" -> version.value)
  )

lazy val tests = project
  .in(file("modules/tests"))
  .dependsOn(core, iron, refined, circe)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "skunk-sharp-tests",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit"                           % munitV           % Test,
      "org.typelevel" %% "munit-cats-effect"               % munitCatsEffectV % Test,
      "com.dimafeng"  %% "testcontainers-scala-munit"      % testcontainersV  % Test,
      "com.dimafeng"  %% "testcontainers-scala-postgresql" % testcontainersV  % Test,
      "dev.rolang"    %% "dumbo"                           % dumboV           % Test,
      "org.typelevel" %% "otel4s-core"                     % otel4sV          % Test
    )
  )
