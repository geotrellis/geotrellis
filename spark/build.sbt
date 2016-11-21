import Dependencies._

name := "geotrellis-spark"
libraryDependencies ++= Seq(
  sparkCore % "provided",
  hadoopClient % "provided",
  "com.google.uzaygezen" % "uzaygezen-core" % "0.2",
  pdal,
  logging,
  avro,
  spire,
  monocleCore, monocleMacro,
  chronoscala,
  scalazStream,
  scalatest % "test"
)

resolvers += Resolver.bintrayRepo("daunnc", "maven")

// must use this method of import to avoid cyclic dependency errors
internalDependencyClasspath in Test <++=
  exportedProducts in Compile in LocalProject("raster-testkit")

internalDependencyClasspath in Test <++=
  exportedProducts in Compile in LocalProject("spark-testkit")

fork in Test := false
parallelExecution in Test := false

initialCommands in console :=
  """
  import geotrellis.raster._
  import geotrellis.vector._
  import geotrellis.proj4._
  import geotrellis.spark._
  import geotrellis.spark.util._
  import geotrellis.spark.tiling._
  """
