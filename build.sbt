ThisBuild / tlBaseVersion    := "0.1"
ThisBuild / organization     := "com.ruiandrebatista"
ThisBuild / organizationName := "Rui Batista"
ThisBuild / startYear        := Some(2026)
ThisBuild / licenses         := Seq(License.Apache2)
ThisBuild / developers       := List(
  tlGitHubDev("ragb", "Rui Batista")
)

ThisBuild / scalaVersion   := "3.8.3"

// Pick up locally-published SNAPSHOT artifacts (currently dumbo 0.9.0-SNAPSHOT while upstream PR is pending).
ThisBuild / resolvers += Resolver.mavenLocal
ThisBuild / tlJdkRelease   := Some(17)
ThisBuild / tlFatalWarnings := true

val skunkV           = "1.0.0"
val catsEffectV      = "3.7.0"
val munitV           = "1.2.4"
val munitCatsEffectV = "2.1.0"
val ironV            = "3.0.2"
val testcontainersV  = "0.44.1"
val dumboV           = "0.9.0-SNAPSHOT"
val otel4sV          = "0.16.0"

lazy val root = tlCrossRootProject.aggregate(core, iron, tests)

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

lazy val tests = project
  .in(file("modules/tests"))
  .dependsOn(core, iron)
  .enablePlugins(NoPublishPlugin)
  .settings(
    name := "skunk-sharp-tests",
    libraryDependencies ++= Seq(
      "org.scalameta"   %% "munit"                           % munitV           % Test,
      "org.typelevel"   %% "munit-cats-effect"               % munitCatsEffectV % Test,
      "com.dimafeng"    %% "testcontainers-scala-munit"      % testcontainersV  % Test,
      "com.dimafeng"    %% "testcontainers-scala-postgresql" % testcontainersV  % Test,
      "dev.rolang"      %% "dumbo"                           % dumboV           % Test,
      "org.typelevel"   %% "otel4s-core"                     % otel4sV          % Test
    )
  )
