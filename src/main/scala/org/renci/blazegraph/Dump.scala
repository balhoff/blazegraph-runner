package org.renci.blazegraph

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import org.backuity.clist._

import java.io.{BufferedOutputStream, File, FileOutputStream}

object Dump extends Command(description = "Dump Blazegraph database to an RDF file") with Common with RDFOutputting with GraphSpecific {

  var output = arg[File]()

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val outputStream = new BufferedOutputStream(new FileOutputStream(output))
    val writer = createOutputWriter(outputStream)
    blazegraph.exportStatements(null, null, null, false, writer, graphOpt.map(blazegraph.getValueFactory.createURI).toSeq: _*)
    outputStream.close()
  }

}
