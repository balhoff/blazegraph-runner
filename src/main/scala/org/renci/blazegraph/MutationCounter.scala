package org.renci.blazegraph

import com.bigdata.rdf.changesets.IChangeLog
import com.bigdata.rdf.changesets.IChangeRecord

class MutationCounter extends IChangeLog {

  private var records = 0

  def mutationCount: Int = records

  def changeEvent(record: IChangeRecord) = records += 1

  def close(): Unit = ()

  def transactionAborted(): Unit = ()

  def transactionBegin(): Unit = ()

  def transactionCommited(commitTime: Long): Unit = ()

  def transactionPrepare(): Unit = ()

}