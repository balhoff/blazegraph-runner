package org.renci.blazegraph

import java.io.File
import java.io.FileInputStream

import org.backuity.clist._
import org.openrdf.model.BNode
import org.openrdf.model.Statement
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.Rio
import org.openrdf.rio.helpers.RDFHandlerBase

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  var base = opt[String](default = "")
  var useOntologyGraph = opt[Boolean](default = false, name = "use-ontology-graph")
  var data = arg[File]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle") match {
    case "turtle"   => RDFFormat.TURTLE
    case "rdfxml"   => RDFFormat.RDFXML
    case "ntriples" => RDFFormat.NTRIPLES
    case other      => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val factory = blazegraph.getValueFactory
    val ontGraphOpt = if (useOntologyGraph) findOntologyURI else None
    val graph = ontGraphOpt.orElse(graphOpt).map(factory.createURI).getOrElse(null)
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    blazegraph.add(data, base, inputFormat, graph)
    blazegraph.commit()
    val mutations = mutationCounter.mutationCount
    blazegraph.removeChangeLog(mutationCounter)
    logger.info(s"$mutations changes")
  }

  /**
   * Tries to efficiently find the ontology IRI triple without loading the whole file.
   */
  def findOntologyURI: Option[String] = {
    object Handler extends RDFHandlerBase {
      override def handleStatement(statement: Statement): Unit = if (statement.getObject.stringValue == "http://www.w3.org/2002/07/owl#Ontology" &&
        statement.getPredicate.stringValue == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") throw new FoundTripleException(statement)
    }
    val inputStream = new FileInputStream(data)
    try {
      val parser = Rio.createParser(inputFormat)
      parser.setRDFHandler(Handler)
      parser.parse(inputStream, base)
      // If an ontology IRI triple is found, it will be thrown out
      // in an exception. Otherwise, return None.
      None
    } catch {
      case FoundTripleException(statement) => {
        if (statement.getSubject.isInstanceOf[BNode]) {
          logger.warn(s"Blank node subject for ontology triple: $statement")
          None
        } else Option(statement.getSubject.stringValue)
      }
    } finally {
      inputStream.close()
    }
  }

  final case class FoundTripleException(statement: Statement) extends RuntimeException

}
