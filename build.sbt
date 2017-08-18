enablePlugins(JavaAppPackaging)

organization  := "org.renci"

name          := "blazegraph-runner"

version       := "1.2"

licenses := Seq("BSD-3-Clause" -> url("https://opensource.org/licenses/BSD-3-Clause"))

homepage := Some(url("https://github.com/balhoff/blazegraph-runner"))

scalaVersion  := "2.12.3"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

scalacOptions in Test ++= Seq("-Yrangepos")

mainClass in Compile := Some("org.renci.blazegraph.Main")

javaOptions += "-Xmx20G"

fork in Test := true

libraryDependencies ++= {
  Seq(
    "com.blazegraph"              %  "bigdata-core"           % "2.1.4" exclude("org.slf4j", "slf4j-log4j12"),
    "org.backuity.clist"          %% "clist-core"             % "3.2.2",
    "org.backuity.clist"          %% "clist-macros"           % "3.2.2" % "provided",
    "com.typesafe.akka"           %% "akka-stream"            % "2.5.3",
    "org.geneontology"            %% "arachne"                % "1.1.1",
    "com.typesafe.scala-logging"  %% "scala-logging"          % "3.7.1",
    "ch.qos.logback"              %  "logback-classic"        % "1.2.3",
    "org.codehaus.groovy"         %  "groovy-all"             % "2.4.6",
    "org.apache.jena"             %  "apache-jena-libs"       % "3.2.0" pomOnly()
  )
}
