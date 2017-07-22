package org.renci.blazegraph

import java.io.File

import org.backuity.clist._
import org.openrdf.rio.RDFFormat

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  var base = opt[String](default = "")
  var data = arg[File]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle") match {
    case "turtle"   => RDFFormat.TURTLE
    case "rdfxml"   => RDFFormat.RDFXML
    case "ntriples" => RDFFormat.NTRIPLES
    case other      => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val factory = blazegraph.getValueFactory
    val graph = graphOpt.map(factory.createURI).getOrElse(null)
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    blazegraph.add(data, base, inputFormat, graph)
    val mutations = mutationCounter.mutationCount
    blazegraph.commit()
    blazegraph.removeChangeLog(mutationCounter)
    logger.info(s"$mutations changes")
  }

}
