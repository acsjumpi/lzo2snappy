lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "br.com.brainboss",
      scalaVersion := "2.11.12",
      version      := "0.0.1"
    )),
    name := "lzordd",
    resolvers ++= Seq("twttr.com" at "https://maven.twttr.com/"),
    libraryDependencies ++= Seq("com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17",
      "org.apache.spark" %% "spark-core" % "2.4.0" %"provided",
      "org.apache.spark" %% "spark-sql" % "2.4.0" %"provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % "1.9.2",
      "org.apache.hadoop" % "hadoop-client" % "3.0.0" %"provided",
    ),
    assemblyJarName in assembly := "lzordd.jar",
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
      ShadeRule.rename("com.chuusai.shapeless.**" -> "shade.com.chuusai.shapeless.@1").inAll
    ),
    test in assembly := {}
  )