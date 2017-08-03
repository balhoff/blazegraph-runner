package org.renci.blazegraph

import java.io.File
import java.io.FileInputStream

import scala.collection.JavaConverters._
import org.backuity.clist._
import org.openrdf.model._
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.Rio
import org.openrdf.rio.helpers.RDFHandlerBase
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import org.apache.commons.io.FileUtils
import org.apache.jena.system.JenaSystem
import org.backuity.clist
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.{AnyNode, BlankNode, ConcreteNode, Node, RuleEngine, Triple, Variable}
import org.geneontology.rules.util.Bridge
import org.openrdf.model.impl.LinkedHashModel
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.parameters.Imports

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  type Blazegraph = BigdataSailRepositoryConnection

  var base = opt[String](default = "")
  var useOntologyGraph = opt[Boolean](default = false, name = "use-ontology-graph")
  var ontology = opt[Option[File]]()
  var data = clist.arg[File]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle") match {
    case "turtle"   => RDFFormat.TURTLE
    case "rdfxml"   => RDFFormat.RDFXML
    case "ntriples" => RDFFormat.NTRIPLES
    case other      => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }

  def runUsingConnection(blazegraph: Blazegraph): Unit = {

    JenaSystem.init()

    val factory = blazegraph.getValueFactory

    //Fix this gross thing. Must be better scala way?
    val ontGraphOpt: String = if (useOntologyGraph && !data.isDirectory) {
      findOntologyURI(data) match {
        case some: Some[String] => some.get
        case None => data.toURI.toURL.toString
      }
    } else {
      graphOpt match {
        case some: Some[String] => some.get
        case None => data.toURI.toURL.toString
      }
    }
    logger.info(s"Graph URI is $ontGraphOpt")
    val assertedTriples = makeTriples(graphFromFile(data, factory))
    loadTriplesToBlazegraph(blazegraph, assertedTriples, ontGraphOpt)

    if(ontology.isDefined) {
      logger.info(s"Using ontology $ontology")
      logger.info("performing reasoning...")
      val inferredGraphName = reasonedUri(ontGraphOpt)
      logger.info(s"Inferred graph URI is $inferredGraphName")
      val manager = OWLManager.createOWLOntologyManager()
      logger.info("Creating OWLManager")
      val owlOntology = manager.loadOntologyFromOntologyDocument(ontology.get)
      val ontologySize = owlOntology.getSignature.size()
      logger.info(s"Ontology loaded, $ontologySize terms")
      val reasoned = reasonedTriples(owlOntology, assertedTriples)
      val numberReasoned = reasoned.size
      logger.info(s"Reasoned has $numberReasoned statements")
      loadGraphToBlazegraph(blazegraph, graphFromFile(ontology.get, factory), findOntologyURI(ontology.get).get)
      loadTriplesToBlazegraph(blazegraph, reasoned, inferredGraphName)
    }
  }

  def graphFromFile(file: File, factory: ValueFactory): Graph = {

    logger.info(s"Parsing $file...")
    var graph: Graph = new LinkedHashModel()

    object LoadHandler extends RDFHandlerBase {
      var graph = new LinkedHashModel()
      override def handleStatement(st: Statement): Unit = {
        graph.add(st)
      }
    }

    def parseFile(file: File): Graph = {
      val inputStream = new FileInputStream(file)
      val parser = Rio.createParser(inputFormat)
      parser.setRDFHandler(LoadHandler)

      try {
        parser.parse(inputStream, base)
      } finally {
        inputStream.close()
      }
      LoadHandler.graph
    }

    if(file.isDirectory) {
      val dataFiles = FileUtils.listFiles(file, Array("ttl"), true).asScala.filter(_.isFile).toArray
      dataFiles.map(parseFile).foldLeft(graph) { (folded, el) =>
        folded.addAll(el)
        folded
      }
    } else {
      parseFile(file)
    }
  }

  def loadTriplesToBlazegraph(blazegraph: Blazegraph, triples: Iterable[Triple], graphUri: String): Unit = {

    logger.info("Loading statements into database...")
    val mutationCounter = new MutationCounter()
    val statements = makeStatements(blazegraph, triples)
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    blazegraph.add(statements, blazegraph.getValueFactory.createURI(graphUri))
    blazegraph.commit()
    val mutations = mutationCounter.mutationCount
    blazegraph.removeChangeLog(mutationCounter)
    logger.info(s"$mutations changes")
  }

  def loadGraphToBlazegraph(blazegraph: Blazegraph, graph: Graph, graphUri: String): Unit = {

    logger.info("Loading statements into database...")
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    blazegraph.add(graph, blazegraph.getValueFactory.createURI(graphUri))
    blazegraph.commit()
    val mutations = mutationCounter.mutationCount
    blazegraph.removeChangeLog(mutationCounter)
    logger.info(s"$mutations changes")
  }

  def makeStatements(blazegraph: Blazegraph, triples: Iterable[Triple]): Graph = {
    var g = new LinkedHashModel()
    val f = blazegraph.getValueFactory
    for(triple <- triples) {
      g.add(createStatement(f, triple))
    }
    g
  }

  def makeTriples(statements: Graph): Iterable[Triple] = {
    var triples = Set[Triple]()
    for(statement <- statements.asScala) {
      triples = triples + createTriple(statement)
    }
    triples
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

  def uriToArachne(uri: URI): org.geneontology.rules.engine.URI = org.geneontology.rules.engine.URI(uri.toString)

  def resourceFromArachne(factory: ValueFactory, resource: org.geneontology.rules.engine.Resource): Resource = resource match {
    case blank: BlankNode => factory.createBNode(blank.id)
    case uri: org.geneontology.rules.engine.URI => uriFromArachne(factory, uri)
  }

  def resourceToArachne(resource: Resource): org.geneontology.rules.engine.Resource = resource match {
    case bnode: BNode => BlankNode(bnode.getID)
    case uri: URI => uriToArachne(uri)
  }

  def valueFromArachne(factory: ValueFactory, node: Node): Value = node match {
    case blank: BlankNode => factory.createBNode(blank.toString())
    case AnyNode => () => "AnyNode"
    case uri: org.geneontology.rules.engine.URI => factory.createURI(uri.uri)
    case literal: org.geneontology.rules.engine.Literal => factory.createLiteral(literal.lexicalForm, literal.datatype.uri)
    case variable: Variable => factory.createURI(variable.name)
  }

  def valueToArachne(value: Value): ConcreteNode = value match {
    case bnode: BNode => BlankNode(bnode.getID)
    case uri: URI => uriToArachne(uri)
    case literal: Literal => org.geneontology.rules.engine.Literal(literal.stringValue(), uriToArachne(literal.getDatatype), None)
  }

  def reasonedTriples(ontology: OWLOntology, triples: Iterable[Triple]): Iterable[Triple] = {
    logger.info("Creating rules from ontology")
    val ontologyRules = Bridge.rulesFromJena(OWLtoRules.translate(ontology, Imports.INCLUDED, true, true, false, true))
    val rulesSize = ontologyRules.size
    logger.info(s"Made $rulesSize rules")
    val engine = new RuleEngine(ontologyRules, false)
    logger.info("Processing triples")
    engine.processTriples(triples).facts -- triples
  }

  def reasonedUri(uri: String): String = {
    uri + "_Inferred"
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
