ThisBuild / scalaVersion := "3.3.0"

lazy val root =
  (project in file("."))
    .enablePlugins(JavaAppPackaging)
    .settings(
      name                 := "unidb",
      Compile / mainClass  := Some("io.univalence.unidb.UniDBApp"),
      maintainer := "francois.sarradin@gmail.com",
      libraryDependencies ++= Seq(
        "dev.zio"     %% "zio"            % libVersion.zio,
        "dev.zio"     %% "zio-streams"    % libVersion.zio,
        "dev.zio"     %% "zio-http"       % "3.0.0-RC1",
        "com.lihaoyi" %% "upickle"        % "3.0.0",
        "org.jline"    % "jline-terminal" % libVersion.jline,
        "org.jline"    % "jline-reader"   % libVersion.jline,
        "org.jline"    % "jline-builtins" % libVersion.jline,
        "dev.zio"     %% "zio-test"       % libVersion.zio % Test
      )
    )

lazy val libVersion =
  new {
    val jline = "3.23.0"
    val zio   = "2.0.10"
  }
