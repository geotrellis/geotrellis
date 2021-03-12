import sbt.Keys._

ThisBuild / scalaVersion := Settings.scala212
ThisBuild / organization := "org.locationtech.geotrellis"
ThisBuild / crossScalaVersions := List(Settings.scala213, Settings.scala212, Settings.scala211)

lazy val root = Project("geotrellis", file("."))
  .aggregate(
    accumulo,
    `accumulo-spark`,
    cassandra,
    `cassandra-spark`,
    `doc-examples`,
    gdal,
    `gdal-spark`,
    geotools,
    hbase,
    `hbase-spark`,
    layer,
    macros,
    mdoc,
    proj4,
    raster,
    `raster-testkit`,
    s3,
    `s3-spark`,
    shapefile,
    spark,
    `spark-pipeline`,
    `spark-testkit`,
    store,
    util,
    vector,
    `vector-testkit`,
    vectortile
  )
  .enablePlugins(ScalaUnidocPlugin)
  .settings(Settings.commonSettings)
  .settings(publish / skip := true)
  .settings(ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject -- inProjects(mdoc))

// aggregate of only modules that can build on 2.13, mostly because they do not have Spark dependencies
lazy val only213 = project
  .aggregate(
    accumulo,
//    `accumulo-spark`,
    cassandra,
//    `cassandra-spark`,
//    `doc-examples`,
    gdal,
//    `gdal-spark`,
    geotools,
    hbase,
//    `hbase-spark`,
    layer,
    macros,
    mdoc,
    proj4,
    raster,
    `raster-testkit`,
    s3,
//    `s3-spark`,
    shapefile,
//    spark,
//    `spark-pipeline`,
//    `spark-testkit`,
    store,
    util,
    vector,
    `vector-testkit`,
    vectortile
  )
  .settings(Settings.commonSettings)

lazy val mdoc = project
  .dependsOn(raster)
  .enablePlugins(MdocPlugin)
  .settings(publish / skip := true)
  .settings(Settings.mdoc)

lazy val macros = project
  .settings(Settings.macros)

lazy val vectortile = project
  .dependsOn(vector)
  .settings(Settings.vectortile)

lazy val vector = project
  .dependsOn(proj4, util)
  .settings(Settings.vector)
  .settings(
    Test / unmanagedClasspath ++= (LocalProject("vector-testkit") / Compile / fullClasspath).value
  )

lazy val `vector-testkit` = project
  .dependsOn(vector % Provided)
  .settings(Settings.`vector-testkit`)

lazy val proj4 = project
  .settings(Settings.proj4)
  .settings(javacOptions ++= Seq("-encoding", "UTF-8"))

lazy val raster = project
  .dependsOn(util, macros, vector)
  .settings(Settings.raster)
  .settings(
    Test / unmanagedClasspath ++= (LocalProject("raster-testkit") / Compile / fullClasspath).value
  )
  .settings(
    Test / unmanagedClasspath ++= (LocalProject("vector-testkit") / Compile / fullClasspath).value
  )

lazy val `raster-testkit` = project
  .dependsOn(raster % Provided, vector % Provided)
  .settings(Settings.`raster-testkit`)

lazy val spark = project
  .dependsOn(util, raster, `raster-testkit` % Test, `vector-testkit` % Test, layer, store)
  .settings(Settings.spark)
  .settings(
    // This takes care of a pseudo-cyclic dependency between the `spark` test scope, `spark-testkit`,
    // and `spark` main (compile) scope. sbt is happy with this. IntelliJ requires that `spark-testkit`
    // be added to the `spark` module dependencies manually (via "Open Module Settings" context menu for "spark" module).
    Test / unmanagedClasspath ++= (LocalProject("spark-testkit") / Compile / fullClasspath).value
  )

lazy val `spark-testkit` = project
  .dependsOn(`raster-testkit`, spark)
  .settings(Settings.`spark-testkit`)

lazy val s3 = project
  .dependsOn(store)
  .settings(Settings.s3)

lazy val `s3-spark` = project
  .dependsOn(
    spark % "compile->compile;test->test",  // <-- spark-testkit update should simplify this
    s3,
    `spark-testkit` % Test
  )
  .settings(Settings.`s3-spark`)

lazy val accumulo = project
  .dependsOn(store)
  .settings(Settings.accumulo)

lazy val `accumulo-spark` = project
  .dependsOn(
    `accumulo`,
    spark % "compile->compile;test->test", // <-- spark-testkit update should simplify this
    `spark-testkit` % Test
  )
  .settings(Settings.`accumulo-spark`)

lazy val cassandra = project
  .dependsOn(store)
  .settings(Settings.cassandra)

lazy val `cassandra-spark` = project
  .dependsOn(
    cassandra,
    spark % "compile->compile;test->test", // <-- spark-testkit update should simplify this
    `spark-testkit` % Test
  )
  .settings(Settings.`cassandra-spark`)

lazy val hbase = project
  .dependsOn(store)
  .settings(projectDependencies := { Seq((store / projectID).value.exclude("com.google.protobuf", "protobuf-java")) })
  .settings(Settings.hbase)

lazy val `hbase-spark` = project
  .dependsOn(
    hbase,
    spark % "compile->compile;test->test", // <-- spark-testkit update should simplify this
    `spark-testkit` % Test
  )
  .settings(projectDependencies := { Seq((hbase / projectID).value, (spark / projectID).value.exclude("com.google.protobuf", "protobuf-java")) })
  .settings(Settings.`hbase-spark`)

lazy val `spark-pipeline` = Project(id = "spark-pipeline", base = file("spark-pipeline")).
  dependsOn(spark, `s3-spark`, `spark-testkit` % "test").
  settings(Settings.`spark-pipeline`)

lazy val geotools = project
  .dependsOn(raster, vector, proj4, `vector-testkit` % Test, `raster-testkit` % Test,
    raster % "test->test" // <-- to get rid  of this, move `GeoTiffTestUtils` to the testkit.
  )
  .settings(Settings.geotools)

/* lazy val geomesa = project
  .dependsOn(`spark-testkit` % Test, spark, geotools, `accumulo-spark`)
  .settings(Settings.geomesa)
  .settings(
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12")
  )

lazy val geowave = project
  .dependsOn(
    proj4, raster, layer, store, accumulo,
    `spark-testkit` % Test, geotools
  )
  .settings(Settings.geowave)
  .settings(
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12")
  ) */

lazy val shapefile = project
  .dependsOn(raster, `raster-testkit` % Test)
  .settings(Settings.shapefile)

lazy val util = project
  .settings(Settings.util)

lazy val `doc-examples` = project
  .dependsOn(spark, `s3-spark`, `accumulo-spark`, `cassandra-spark`, `hbase-spark`, spark, `spark-testkit`, `spark-pipeline`)
  .settings(publish / skip := true)
  .settings(Settings.`doc-examples`)

lazy val bench = project
  .dependsOn(raster, spark)
  .enablePlugins(GTBenchmarkPlugin)
  .settings(Settings.bench)

lazy val layer = project
  .dependsOn(raster, `raster-testkit` % Test)
  .settings(Settings.layer)

lazy val store = project
  .dependsOn(layer)
  .settings(Settings.store)

lazy val gdal = project
  .dependsOn(raster, `raster-testkit` % Test)
  .settings(Settings.gdal)

lazy val `gdal-spark` = project
  .dependsOn(gdal, spark, `spark-testkit` % Test)
  .settings(publish / skip := true) // at this point we need this project only for tests
  .settings(Settings.`gdal-spark`)
