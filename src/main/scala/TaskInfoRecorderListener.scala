package com.databricks

import scala.collection.mutable
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.TaskEndReason


object TaskInfoRecorderListener {
  type RecordedMetrics = (Int, Int, String, TaskInfo, TaskMetrics, ExecutorMetrics, TaskEndReason)
}

class TaskInfoRecorderListener extends SparkListener {
  val taskInfoMetrics: mutable.Buffer[TaskInfoRecorderListener.RecordedMetrics] = mutable.Buffer()
  var tracking = false

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    if (tracking && taskEnd.taskInfo != null && taskEnd.taskMetrics != null) {
      taskInfoMetrics += ((taskEnd.stageId,
              taskEnd.stageAttemptId,
              taskEnd.taskType,
              taskEnd.taskInfo,
              taskEnd.taskMetrics,
              taskEnd.taskExecutorMetrics,
              taskEnd.reason))
    }
  }
}
