package org.renci.blazegraph

import java.io.{File, FileInputStream}

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import com.bigdata.rdf.store.DataLoader
import org.apache.commons.io.FileUtils
import org.apache.jena.sys.JenaSystem
import org.backuity.clist._
import org.openrdf.model._
import org.openrdf.rio.helpers.RDFHandlerBase
import org.openrdf.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters._

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  var base = opt[String](default = "")
  var useOntologyGraph = opt[Boolean](default = false, name = "use-ontology-graph")
  var dataFiles = args[Seq[File]]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle") match {
    case "turtle"   => RDFFormat.TURTLE
    case "rdfxml"   => RDFFormat.RDFXML
    case "ntriples" => RDFFormat.NTRIPLES
    case other      => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()
    val tripleStore = blazegraph.getSailConnection.getTripleStore
    val loader = new DataLoader(tripleStore)
    val filesToLoad = dataFiles.flatMap(data => if (data.isFile) List(data) else FileUtils.listFiles(data, inputFormat.getFileExtensions.asScala.toArray, true).asScala).filter(_.isFile)
    filesToLoad.foreach { file =>
      scribe.info(s"Loading $file")
      val ontGraphOpt = if (useOntologyGraph) findOntologyURI(file) else None
      val determinedGraphOpt = ontGraphOpt.orElse(graphOpt)
      val stats = loader.loadFiles(file, base, inputFormat, determinedGraphOpt.getOrElse(file.toURI.toString), null)
      scribe.info(stats.toString)
    }
    loader.endSource()
    tripleStore.commit()
  }

  /**
    * Tries to efficiently find the ontology IRI triple without loading the whole file.
    */
  def findOntologyURI(file: File): Option[String] = {
    object Handler extends RDFHandlerBase {
      override def handleStatement(statement: Statement): Unit = if (statement.getObject.stringValue == "http://www.w3.org/2002/07/owl#Ontology" &&
        statement.getPredicate.stringValue == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") throw FoundTripleException(statement)
    }
    val inputStream = new FileInputStream(file)
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
          scribe.warn(s"Blank node subject for ontology triple: $statement")
          None
        } else Option(statement.getSubject.stringValue)
      }
    } finally {
      inputStream.close()
    }
  }

  final case class FoundTripleException(statement: Statement) extends RuntimeException

}
