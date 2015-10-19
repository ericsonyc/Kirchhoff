package spark

import org.apache.spark.{HashPartitioner, Partitioner}

/**
 * Created by jtang on 2015/1/16 0016.
 */
class ScalaPartitioner(partitions: Int, elemens: Long) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => {
      val rawMod = elemens.## / partitions
      var index = 0;
      if (rawMod != 0) {
        index = key.## / rawMod
        if (index >= partitions) {
          index = partitions - 1
        }
      } else {
        index = key.##
      }

      //println("index:" + index)
      index
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}