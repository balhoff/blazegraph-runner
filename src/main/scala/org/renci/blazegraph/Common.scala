package org.renci.blazegraph

import java.io.File
import java.io.FileInputStream
import java.util.Properties

import org.backuity.clist._

import com.bigdata.journal.Options
import com.bigdata.rdf.sail.BigdataSail
import com.bigdata.rdf.sail.BigdataSailRepository
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import com.typesafe.scalalogging.LazyLogging

trait Common extends Command with LazyLogging {

  var informat = opt[Option[String]](name = "informat", description = "Input format")
  var journalFile = opt[File](name = "journal", default = new File("blazegraph.jnl"), description = "Blazegraph journal file")
  var inputProperties = opt[Option[File]](name = "properties", description = "Blazegraph properties file")
  var outformat = opt[Option[String]](name = "outformat", description = "Output format")

  final def run(): Unit = {
    val blazegraphProperties = new Properties()
    val propertiesStream = inputProperties.map(new FileInputStream(_)).getOrElse(this.getClass.getResourceAsStream("blazegraph.properties"))
    blazegraphProperties.load(propertiesStream)
    propertiesStream.close()
    blazegraphProperties.setProperty(Options.FILE, journalFile.getAbsolutePath)
    val sail = new BigdataSail(blazegraphProperties)
    val repository = new BigdataSailRepository(sail)
    repository.initialize()
    val blazegraph = repository.getUnisolatedConnection()

    runUsingConnection(blazegraph)

    blazegraph.close()
    repository.shutDown()
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit

}