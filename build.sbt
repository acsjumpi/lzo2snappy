lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "br.com.brainboss",
      scalaVersion := "2.11.12",
      version      := "0.0.1"
    )),
    name := "l2s",
    resolvers ++= Seq(
      "twttr.com" at "https://maven.twttr.com/"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.5" %"provided",
      "org.apache.spark" %% "spark-sql" % "2.4.5" %"provided",
      "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17" % "provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % "1.9.4",
      "com.typesafe" % "config" % "1.4.1"
    ),
    assemblyJarName in assembly := "l2s.jar",
    assemblyMergeStrategy in assembly := {
      case x if Assembly.isConfigFile(x) =>
        MergeStrategy.concat
      case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
        MergeStrategy.rename
      case PathList("META-INF", xs @ _*) =>
        xs map {_.toLowerCase} match {
          case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
            MergeStrategy.discard
          case ps @ _ :: _ if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case "plexus" :: _ =>
            MergeStrategy.discard
          case "services" :: _ =>
            MergeStrategy.filterDistinctLines
          case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
            MergeStrategy.filterDistinctLines
          case _ => MergeStrategy.first
        }
      case _ => MergeStrategy.first
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("shapeless.**" -> "shade_shapeless.@1").inAll
    ),
    test in assembly := {}
  )