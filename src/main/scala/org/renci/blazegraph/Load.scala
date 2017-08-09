package org.renci.blazegraph

import java.io.File
import java.io.FileInputStream
import java.util.Collection

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.jena.system.JenaSystem
import org.backuity.clist._
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.BlankNode
import org.geneontology.rules.engine.ConcreteNode
import org.geneontology.rules.engine.RuleEngine
import org.geneontology.rules.engine.Triple
import org.geneontology.rules.util.Bridge
import org.openrdf.model._
import org.openrdf.model.impl.URIImpl
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.Rio
import org.openrdf.rio.helpers.RDFHandlerBase
import org.openrdf.rio.helpers.StatementCollector
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.parameters.Imports

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  var base = opt[String](default = "")
  var reason = opt[Option[File]](description = "Ontology that will be formed into rules for reasoning on input files")
  var isDefault = opt[Boolean](default = false, name = "default")
  var dataFiles = args[Seq[File]]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle") match {
    case "turtle"   => RDFFormat.TURTLE
    case "rdfxml"   => RDFFormat.RDFXML
    case "ntriples" => RDFFormat.NTRIPLES
    case other      => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()
    val factory = blazegraph.getValueFactory
    val filesToLoad = dataFiles.flatMap( data => if(data.isFile) List(data) else FileUtils.listFiles(data, inputFormat.getFileExtensions.asScala.toArray, true).asScala).filter(_.isFile)
    reason match {
      case Some(ontFile) =>
        val arachne = new RuleEngine(Bridge.rulesFromJena(OWLtoRules.translate(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(ontFile), Imports.INCLUDED, true, true, false, true)), false)
        filesToLoad.foreach { file =>
          logger.info(s"Loading and reasoning $file")
          val collector = new StatementCollector()
          val parser = Rio.createParser(inputFormat)
          parser.setRDFHandler(collector)
          val inputStream = new FileInputStream(file)
          parser.parse(inputStream, base)
          inputStream.close()
          val graphName = determineGraph(file)
          val loaded = loadStatementsToBlazegraph(blazegraph, collector.getStatements, graphName)
          logger.info(s"$loaded loaded (asserted)")
          val assertedTriples = collector.getStatements.asScala.map(createTriple)
          val inferredTriples = arachne.processTriples(assertedTriples).facts -- assertedTriples
          val inferredStatements = inferredTriples.map(createStatement(factory, _))
          val loadedInferred = loadStatementsToBlazegraph(blazegraph, inferredStatements.asJava, reasonedURI(graphName))
          logger.info(s"$loadedInferred loaded (inferred)")
        }
        val ontologyLoaded = loadFileToBlazegraph(blazegraph, ontFile, Option(determineGraph(ontFile)))
        logger.info(s"$ontologyLoaded loaded (ontology)")

      case None => filesToLoad.foreach { file =>
        logger.info(s"Loading $file")
        val graphUri: Option[URI] = if(this.isDefault) None else if(graphOpt.isDefined) graphOpt.map(new URIImpl(_)) else Option(determineGraph(file))
        val mutations = loadFileToBlazegraph(blazegraph, file, graphUri)
        logger.info(s"$mutations changes")
      }
    }
  }

  private def loadStatementsToBlazegraph(blazegraph: BigdataSailRepositoryConnection, statements: Collection[Statement], graph: URI): Int = {
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    blazegraph.add(statements, graph)
    blazegraph.commit()
    val mutations = mutationCounter.mutationCount
    blazegraph.removeChangeLog(mutationCounter)
    mutations
  }

  private def loadFileToBlazegraph(blazegraph: BigdataSailRepositoryConnection, file: File, graph: Option[URI]): Int = {
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    graph match {
      case Some(g) => blazegraph.add(file, base, inputFormat, g)
      case None => blazegraph.add(file, base, inputFormat)
    }
    blazegraph.commit()
    blazegraph.removeChangeLog(mutationCounter)
    mutationCounter.mutationCount
  }

  def createStatement(factory: ValueFactory, triple: Triple): Statement = {
    val subject: Resource = resourceFromArachne(factory, triple.s)
    val predicate: URI = uriFromArachne(factory, triple.p)
    val obj: Value = valueFromArachne(factory, triple.o)
    factory.createStatement(subject, predicate, obj)
  }

  def createTriple(statement: Statement): Triple = {
    Triple(resourceToArachne(statement.getSubject), uriToArachne(statement.getPredicate), valueToArachne(statement.getObject))
  }

  def uriFromArachne(factory: ValueFactory, uri: org.geneontology.rules.engine.URI): URI = factory.createURI(uri.uri)

  def uriToArachne(uri: URI): org.geneontology.rules.engine.URI = org.geneontology.rules.engine.URI(uri.stringValue)

  def resourceFromArachne(factory: ValueFactory, resource: org.geneontology.rules.engine.Resource): Resource = resource match {
    case uri @ org.geneontology.rules.engine.URI(_) => uriFromArachne(factory, uri)
    case BlankNode(id)                              => factory.createBNode(id)
  }

  def resourceToArachne(resource: Resource): org.geneontology.rules.engine.Resource = resource match {
    case uri: URI     => uriToArachne(uri)
    case bnode: BNode => BlankNode(bnode.getID)
  }

  def valueFromArachne(factory: ValueFactory, node: ConcreteNode): Value = node match {
    case org.geneontology.rules.engine.URI(value) => factory.createURI(value)
    case BlankNode(id) => factory.createBNode(id)
    case org.geneontology.rules.engine.Literal(value, _, Some(lang)) => factory.createLiteral(value, lang)
    case org.geneontology.rules.engine.Literal(value, uri @ org.geneontology.rules.engine.URI(_), _) => factory.createLiteral(value, uriFromArachne(factory, uri))
  }

  def valueToArachne(value: Value): ConcreteNode = value match {
    case bnode: BNode     => BlankNode(bnode.getID)
    case uri: URI         => uriToArachne(uri)
    case literal: Literal => org.geneontology.rules.engine.Literal(literal.stringValue(), uriToArachne(literal.getDatatype), Option(literal.getLanguage))
  }

  def reasonedURI(uri: URI): URI = new URIImpl(uri.stringValue + "_Inferred")

  def determineGraph(file: File): URI = new URIImpl(findOntologyURI(file).getOrElse(file.toURI.toURL.toString))

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
