package org.renci.blazegraph

import java.io.File

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration

import org.apache.jena.system.JenaSystem
import org.backuity.clist._
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.RuleEngine
import org.geneontology.rules.util.Bridge
import org.openrdf.model.Statement
import org.openrdf.model.impl.URIImpl
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.parameters.Imports

import com.bigdata.rdf.sail.BigdataSailRepository
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import scala.util.Success
import scala.util.Failure
import com.typesafe.scalalogging.LazyLogging

object Reason extends Command(description = "Materialize inferences") with Common with LazyLogging {

  var appendGraphName = opt[String](default = "#inferred")
  var targetGraph = opt[Option[String]]()
  var mergeSources = opt[Boolean](default = false)
  var ontology = opt[File](default = new File("ro.owl"))
  var rulesFile = opt[Option[File]]() // Not used yet
  var parallelism = opt[Int](default = Runtime.getRuntime().availableProcessors)
  var sourceGraphs = args[List[String]]()

  private def computedTargetGraph(graph: String): String = targetGraph.getOrElse {
    if (mergeSources) inferredGraphName(sourceGraphs.head)
    else inferredGraphName(graph)
  }

  private def inferredGraphName(graph: String): String = s"$graph$appendGraphName"

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()
    implicit val system = ActorSystem("Reason")
    implicit val materializer = ActorMaterializer()
    val ont = OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(ontology)
    val arachne = new RuleEngine(Bridge.rulesFromJena(OWLtoRules.translate(ont, Imports.INCLUDED, true, true, true, true)), false)
    ont.getOWLOntologyManager.removeOntology(ont)
    val repository = blazegraph.getRepository
    val sourceGraphGroups = if (mergeSources) List(sourceGraphs -> computedTargetGraph(sourceGraphs.head))
    else sourceGraphs.map(g => List(g) -> computedTargetGraph(g))
    val done = Source(sourceGraphGroups)
      .mapAsyncUnordered(parallelism) {
        case (graphs, targetGraph) =>
          logger.debug(s"Loading from $graphs for target $targetGraph")
          statementsForGraphs(graphs, repository).map(_ -> targetGraph)
      }
      .mapAsyncUnordered(parallelism) {
        case (statements, targetGraph) => Future {
          logger.debug(s"Reasoning for $targetGraph")
          val triples = statements.map(ArachneBridge.createTriple)
          val wm = arachne.processTriples(triples)
          val inferred = wm.facts -- wm.asserted
          val res = inferred.map(ArachneBridge.createStatement(blazegraph.getValueFactory, _)) -> targetGraph
          logger.debug(s"Done reasoning $targetGraph")
          res
        }
      }
      .runForeach {
        case (statements, targetGraph) =>
          logger.debug(s"Runforeach $targetGraph")
          blocking {
            logger.debug(s"Inserting result into $targetGraph")
            val mutationCounter = new MutationCounter()
            blazegraph.addChangeLog(mutationCounter)
            blazegraph.begin()
            blazegraph.add(statements.asJava, new URIImpl(targetGraph))
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

  def statementsForGraphs(graphs: Seq[String], repository: BigdataSailRepository): Future[Set[Statement]] = Future {
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