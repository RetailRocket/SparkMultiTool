package ru.retailrocket.spark.multitool

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import java.io.{BufferedReader, InputStreamReader, FileInputStream, FileOutputStream, File}
import java.nio.file.{FileAlreadyExistsException}


package object fs {
  val DefaultTempPath = "/tmp/spark"

  val DefaultCodec = classOf[org.apache.hadoop.io.compress.GzipCodec]

  def actionViaTemp(output: String, tempPath: Option[String]=None)(action: String => Unit)(store: (String, String) => Unit): Unit = {
    val tempRoot = tempPath getOrElse DefaultTempPath
    val temp = "%s_%d".format(tempRoot, System.currentTimeMillis/1000)
    action(temp)
    store(temp, output)
  }

  def saveViaTemp(src: RDD[_])(output: String, tempPath: Option[String]=None, codec: Option[Class[_ <: CompressionCodec]]=None)(store: (String, String) => Unit): Unit = {
    actionViaTemp(output, tempPath) { path => src.saveAsTextFile(path, codec getOrElse DefaultCodec) } (store)
  }

  def saveViaTempWithReplace(src: RDD[_])(output: String, tempPath: Option[String]=None, codec: Option[Class[_ <: CompressionCodec]]=None): Unit = {
    saveViaTemp(src)(output, tempPath, codec) (replace _)
  }

  def saveViaTempWithRename(src: RDD[_])(output: String, tempPath: Option[String], codec: Option[Class[_ <: CompressionCodec]]=None): Unit = {
    saveViaTemp(src)(output, tempPath, codec) (rename _)
  }

  def exists(dst: String): Boolean = {
    val fs = FileSystem.get(new Configuration())
    val dstPath = new Path(dst)
    fs.exists(dstPath)
  }

  def delete(dst: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    val dstPath = new Path(dst)
    if(fs.exists(dstPath)) fs.delete(dstPath, true)
  }

  def rename(src: String, dst: String) = {
    val fs = FileSystem.get(new Configuration())
    val dstPath = new Path(dst)
    val srcPath = new Path(src)
    if(fs.exists(dstPath)) throw new FileAlreadyExistsException(s"path already exists - ${dst}")
    fs.rename(srcPath, dstPath)
  }

  def replace(src: String, dst: String) = {
    val fs = FileSystem.get(new Configuration())
    val srcPath = new Path(src)
    val dstPath = new Path(dst)
    if(fs.exists(dstPath)) fs.delete(dstPath, true)
    fs.rename(srcPath, dstPath)
  }

  def storeLocal(data: String, path: String) {
    val out = new FileOutputStream(path)
    val bytes = data.getBytes
    out.write(bytes, 0, bytes.size)
    out.close()
  }

  def storeHdfs(data: String, path: String) {
    val fs = FileSystem.get(new Configuration())
    val out = fs.create(new Path(path))
    val bytes = data.getBytes
    out.write(bytes, 0, bytes.size)
    out.close()
  }

  def createTempDirectoryLocal(prefix: String): String = {
    val temp = File.createTempFile(prefix, "")
    temp.delete()
    temp.mkdir()
    temp.getPath
  }
}
