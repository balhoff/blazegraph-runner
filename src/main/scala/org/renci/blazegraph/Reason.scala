package org.renci.blazegraph

import java.io.File

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.control.NonFatal

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.system.JenaSystem
import org.backuity.clist._
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.RuleEngine
import org.geneontology.rules.util.Bridge
import org.openrdf.model.Statement
import org.openrdf.model.impl.URIImpl
import org.openrdf.query.QueryLanguage
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.RioRDFXMLDocumentFormatFactory
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.OWLOntologyLoaderConfiguration
import org.semanticweb.owlapi.model.parameters.Imports
import org.semanticweb.owlapi.rio.RioMemoryTripleSource
import org.semanticweb.owlapi.rio.RioParserImpl

import com.bigdata.rdf.sail.BigdataSailRepository
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import com.typesafe.scalalogging.LazyLogging

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

object Reason extends Command(description = "Materialize inferences") with Common with LazyLogging {

  var targetGraph = opt[Option[String]](description = "Named graph to store inferred statements.")
  var appendGraphName = opt[String](default = "#inferred", description = "If a target-graph is not provided, append this text to the end of source graph name to use as target graph for inferred statements.")
  var mergeSources = opt[Boolean](default = false, description = "Merge all selected source graphs into one set of statements before reasoning. Inferred statements will be stored in provided `target-graph`, or else in the default graph. If `merge-sources` is false (default), source graphs will be reasoned separately and in parallel.")
  var ontology = opt[Option[String]](description = "Ontology to use as rule source. If the passed value is a valid filename, the ontology will be read from the file. Otherwise, if the value is an ontology IRI, it will be loaded from the database if such a graph exists, or else, from the web.")
  var rulesFile = opt[Option[File]](description = "Reasoning rules in Jena syntax.")
  var parallelism = opt[Int](default = Math.min(Runtime.getRuntime().availableProcessors / 2, 2), description = "Maximum graphs to simultaneously either read from database or run reasoning on.")
  var sourceGraphs = arg[String](description = "File name or query text of SPARQL select used to obtain graph names on which to perform reasoning. The query must return a column named `source_graph`.")

  private def computedTargetGraph(graph: String): Option[String] = targetGraph.orElse {
    if (mergeSources) None
    else Option(inferredGraphName(graph))
  }

  private def inferredGraphName(graph: String): String = s"$graph$appendGraphName"

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()

    val ontRules = ontology.flatMap(tryLoadOntologyFromFile)
      .orElse(ontology.flatMap(tryLoadOntologyFromDatabase(_, blazegraph.getRepository)))
      .orElse(ontology.flatMap(tryLoadOntologyFromWeb))
      .map(ont => Bridge.rulesFromJena(OWLtoRules.translate(ont, Imports.INCLUDED, true, true, false, true)).toSet)
      .getOrElse(Set.empty)
    val extraRules = rulesFile.map(file => Bridge.rulesFromJena(Rule.parseRules(io.Source.fromFile(file, "utf-8").mkString).asScala).toSet).getOrElse(Set.empty)
    val allRules = ontRules ++ extraRules
    if (allRules.isEmpty) {
      logger.error(s"No rules provided.")
      System.exit(1)
    }
    implicit val system = ActorSystem("Reason")
    implicit val materializer = ActorMaterializer()
    val arachne = new RuleEngine(allRules, false)

    val sourcesQueryString = if (new File(sourceGraphs).exists) io.Source.fromFile(sourceGraphs, "utf-8").mkString else sourceGraphs
    val query = blazegraph.prepareTupleQuery(QueryLanguage.SPARQL, sourcesQueryString)
    val sourcesQueryResult = query.evaluate()
    var sourceGraphNames = List.empty[String]
    while (sourcesQueryResult.hasNext()) {
      sourceGraphNames = sourcesQueryResult.next().getValue("source_graph").stringValue :: sourceGraphNames
    }
    logger.info(s"Reasoning on source graphs: \n${sourceGraphNames.mkString("\n")}")
    sourcesQueryResult.close()
    val sourceGraphGroups = if (mergeSources) List(sourceGraphNames -> computedTargetGraph(sourceGraphNames.head))
    else sourceGraphNames.map(g => List(g) -> computedTargetGraph(g))

    // Now process each graph group in 3 stages: 
    // (1) load graph group statements from database 
    // (2) run reasoner over statements
    // (3) insert inferred statements into database
    // Each of these will happen in parallel up to the maximum 'parallelism' value, but early stages will wait
    // to start a new graph group if downstream stages are busy.
    val done = Source(sourceGraphGroups)
      .mapAsyncUnordered(parallelism) {
        case (graphs, targetGraph) =>
          logger.debug(s"Loading from $graphs for target $targetGraph")
          statementsForGraphs(graphs, blazegraph.getRepository).map((_, targetGraph))
      }
      .mapAsyncUnordered(parallelism) {
        case (statements, targetGraph) => Future {
          logger.debug(s"Reasoning for $targetGraph")
          val triples = statements.map(ArachneBridge.createTriple)
          val wm = arachne.processTriples(triples)
          val inferred = wm.facts -- wm.asserted
          val result = (inferred.map(ArachneBridge.createStatement(blazegraph.getValueFactory, _)), targetGraph)
          logger.debug(s"Done reasoning $targetGraph")
          result
        }
      }
      .runForeach {
        case (statements, targetGraph) =>
          logger.debug(s"Inserting result into $targetGraph")
          blocking {
            val mutationCounter = new MutationCounter()
            blazegraph.addChangeLog(mutationCounter)
            blazegraph.begin()
            blazegraph.add(statements.asJava, targetGraph.map(new URIImpl(_)).get)
            blazegraph.commit()
            val mutations = mutationCounter.mutationCount
            blazegraph.removeChangeLog(mutationCounter)
            logger.info(s"$mutations changes in $targetGraph")
          }
      }
    Await.ready(done, Duration.Inf).onComplete {
      case Failure(e) =>
        e.printStackTrace()
        system.terminate()
        System.exit(1)
      case _ => system.terminate()
    }
  }

  private def tryLoadOntologyFromFile(filename: String): Option[OWLOntology] = try {
    val file = new File(filename)
    if (file.exists) Option(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(file)) else None
  } catch {
    case NonFatal(e) => None
  }

  private def tryLoadOntologyFromDatabase(graphName: String, repository: BigdataSailRepository): Option[OWLOntology] = try {
    val manager = OWLManager.createOWLOntologyManager()
    val statements = Await.result(statementsForGraphs(List(graphName), repository), Duration.Inf)
    val source = new RioMemoryTripleSource(statements.asJava)
    val parser = new RioParserImpl(new RioRDFXMLDocumentFormatFactory())
    val newOnt = manager.createOntology()
    //TODO what will happen with imports? Ignored, or loaded from web??
    parser.parse(source, newOnt, new OWLOntologyLoaderConfiguration())
    Option(newOnt)
  } catch {
    case NonFatal(e) => None
  }

  private def tryLoadOntologyFromWeb(ontIRI: String): Option[OWLOntology] = try {
    val manager = OWLManager.createOWLOntologyManager()
    Option(manager.loadOntology(IRI.create(ontIRI)))
  } catch {
    case NonFatal(e) => None
  }

  private def statementsForGraphs(graphs: Seq[String], repository: BigdataSailRepository): Future[Set[Statement]] = Future {
    blocking {
      val connection = repository.getReadOnlyConnection
      val result = connection.getStatements(null, null, null, false, graphs.map(new URIImpl(_)): _*)
      var statements = Set.empty[Statement]
      while (result.hasNext) statements += result.next
      result.close()
      connection.close()
      statements
    }
  }

}