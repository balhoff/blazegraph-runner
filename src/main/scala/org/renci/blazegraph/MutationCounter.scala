package org.renci.blazegraph


import com.bigdata.rdf.changesets.IChangeRecord
import com.bigdata.rdf.changesets.IChangeLog
import com.typesafe.scalalogging.LazyLogging

class MutationCounter extends IChangeLog with LazyLogging{

  private var records = 0

  def mutationCount = records

  def changeEvent(record: IChangeRecord) = records += 1

  def close(): Unit = ()

  def transactionAborted(): Unit = ()

  def transactionBegin(): Unit = {
    logger.info("Here we bigin a transaction")
  }

  def transactionCommited(commitTime: Long): Unit = {
    logger.info("Here we commit a transaction")
  }

  def transactionPrepare(): Unit = ()

}