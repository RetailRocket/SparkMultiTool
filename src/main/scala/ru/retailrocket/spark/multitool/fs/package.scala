package ru.retailrocket.spark.multitool

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration


package object fs {
  def replace(src: String, dst: String) = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    val fs = FileSystem.get(new Configuration())
    if(fs.exists(dstPath)) fs.delete(dstPath, true)
    fs.rename(srcPath, dstPath)
  }
}
