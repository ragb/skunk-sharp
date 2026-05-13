addSbtPlugin("org.typelevel" % "sbt-typelevel"         % "0.8.5")
addSbtPlugin("org.typelevel" % "sbt-typelevel-mergify" % "0.8.5")
addSbtPlugin("org.typelevel" % "sbt-typelevel-site"    % "0.8.5")

// Brings the typed `ScalacOptions.other("-preview", _ >= ScalaVersion(3, 8, 0))` API into build.sbt scope so we can
// add `-preview` (SIP-71 `into` modifier) as a typed option rather than a raw string.
libraryDependencies += "org.typelevel" %% "scalac-options" % "0.1.7"
