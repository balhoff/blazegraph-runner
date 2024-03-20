enablePlugins(JavaAppPackaging)

organization := "org.renci"

name := "blazegraph-runner"

version := "1.7"

licenses := Seq("BSD-3-Clause" -> url("https://opensource.org/licenses/BSD-3-Clause"))

homepage := Some(url("https://github.com/balhoff/blazegraph-runner"))

scalaVersion := "2.13.11"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in Compile := Some("org.renci.blazegraph.Main")

javaOptions += "-Xmx20G"

fork in Test := true

libraryDependencies ++= {
  Seq(
    "com.blazegraph"              %  "bigdata-core"           % "2.1.4" exclude("org.slf4j", "slf4j-log4j12"),
    "net.sourceforge.owlapi"      %  "owlapi-distribution"    % "4.5.16",
    "org.backuity.clist"          %% "clist-core"             % "3.5.1",
    "org.backuity.clist"          %% "clist-macros"           % "3.5.1" % "provided",
    "com.typesafe.akka"           %% "akka-stream"            % "2.6.20",
    "org.geneontology"            %% "arachne"                % "1.3",
    "org.geneontology"            %% "whelk-owlapi"           % "1.1.2",
    "com.outr"                    %% "scribe-slf4j"           % "3.13.1",
    "org.apache.jena"             %  "apache-jena-libs"       % "3.17.0" pomOnly(),
    // These are required for certain blazegraph parsers on Java 11
    "com.sun.xml.bind"            % "jaxb-core"               % "2.3.0.1",
    "javax.xml.bind"              % "jaxb-api"                % "2.3.1",
    "com.sun.xml.bind"            % "jaxb-impl"               % "2.3.8"
  )
}

// Later OWL API versions are not compatible with Blazegraph's Sesame dependency
dependencyOverrides += "net.sourceforge.owlapi" % "owlapi-distribution" % "4.5.16"
