package org.renci.blazegraph

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

import org.backuity.clist._
import org.openrdf.query.QueryLanguage

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Dump extends Command(description = "Dump Blazegraph database to an RDF file") with Common with RDFOutputting with GraphSpecific {

  var output = arg[File]()

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val graph = graphOpt.map(g => s"FROM <$g>").getOrElse("")
    val query = blazegraph.prepareGraphQuery(QueryLanguage.SPARQL, s"CONSTRUCT $graph WHERE { ?s ?p ?o . }")
    val queryOutput = new BufferedOutputStream(new FileOutputStream(output))
    query.evaluate(createOutputWriter(queryOutput))
    queryOutput.close()
  }

}