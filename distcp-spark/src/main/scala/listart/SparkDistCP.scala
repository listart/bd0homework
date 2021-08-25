package listart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.sys.exit

object SparkDistCP {
  val usage =
    """
  SparkDistCp [-i] [-m] source_dir target_dir
  -i Ignore failures
  -m max concurrence
"""

  type OptionMap = Map[Symbol, Any]

  @tailrec
  def getOpt(map: OptionMap, args: List[String]): OptionMap = {
    args match {
      case Nil => map
      case "-i" :: tail => getOpt(map ++ Map('ignoreFailures -> true), tail)
      case "-m" :: value :: tail => getOpt(map ++ Map('maxConcurrence -> value.toInt), tail)
      case sourceDir :: targetDir :: Nil => map ++ Map('sourceDir -> sourceDir, 'targetDir -> targetDir)
      case option :: tail => println(s"Unknown option $option")
        exit(1)
    }
  }

  def getFileSystem(): FileSystem = {
    val conf = new Configuration()
    conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"))

    FileSystem.get(conf)
  }

  def getFileList(fs: FileSystem, dir: String): Array[String] = {
    println(dir)

    val fileList = new ListBuffer[String]

    val dirPath = new Path(dir)
    val dirStatus = fs.getFileStatus(dirPath)

    if (dirStatus.isDirectory) {
      val contextUri = dirStatus.getPath.toUri
      val iterator = fs.listFiles(dirPath, true)
      while (iterator.hasNext) {
        val fileUri = iterator.next().getPath.toUri
        fileList += contextUri.relativize(fileUri).toString
      }
    }

    fileList.toArray
  }

  def listDirectories(fs: FileSystem, contextDir: Path, currentDir: Path): Seq[Path] = {
    fs.listStatus(currentDir)
      .filter(_.isDirectory)
      .flatMap { s =>
        val subDir = s.getPath
        val relativePath = contextDir.toUri.relativize(subDir.toUri)
        listDirectories(fs, contextDir, s.getPath) ++ Seq(new Path(relativePath))
      }
  }

  def copyFile(fs: FileSystem, srcPath: String, dstPath: String, partition: Int, relativePath: String):String = {
    val src = s"$srcPath/$relativePath"
    val dst = s"$dstPath/$relativePath"

    val logger = Logger.getLogger("SparkDistCP")
    logger.info(s"[$partition] cp $src $dst")

    FileUtil.copy(fs, new Path(src), fs, new Path(dst), false, true, fs.getConf)

    dst
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(usage)
      return
    }

    val opts = getOpt(Map(), args.toList)
    val srcDir = opts.getOrElse('sourceDir, "Unknown Source dir").toString
    val dstDir = opts.getOrElse('targetDir, "Unknown target dir").toString
    val ignoreFailures = opts.getOrElse('ignoreFailures, true).asInstanceOf[Boolean]
    val maxConcurrence = opts.getOrElse('maxConcurrence, 8).asInstanceOf[Int]
    println(s"accepts arguments $opts")

    // check source directory
    val fs = getFileSystem()
    if (!fs.exists(new Path(srcDir))) {
      println(s"not exists source directory $srcDir")
      exit(1)
    }
    val srcPath = fs.getFileStatus(new Path(srcDir)).getPath
    val source = srcPath.toString

    // check target directory
    if (!fs.exists(new Path(dstDir))) {
      fs.mkdirs(new Path(dstDir))
      println(s"created directory $dstDir")
    }
    val dstStatus = fs.getFileStatus(new Path(dstDir))
    if (!dstStatus.isDirectory) {
      println(s"target directory $dstDir is not a directory")
      exit(1)
    }
    val dstPath = fs.getFileStatus(new Path(dstDir)).getPath
    val target = dstPath.toString

    // list all sub directories of source directory
    val allDirs = listDirectories(fs, srcPath, srcPath)
    val allDstDirs = allDirs.map(r => new Path(s"$dstPath/$r"))

    // check exceptions for target directories are file
    val badDirs = allDstDirs.filter(p => fs.exists(p) && !fs.getFileStatus(p).isDirectory)
    if (badDirs.nonEmpty && !ignoreFailures) {
      val badDirList = badDirs mkString ","
      println(s"invalidate target directories: $badDirList")
      exit(1)
    }

    // try to make not exists target directories
    allDstDirs.foreach { p =>
      if (!fs.exists(p)) {
        fs.mkdirs(p)
        println(s"created directory ${p.toString}")
      }
    }

    val fileList = getFileList(fs, srcDir)

    val conf = new SparkConf().setMaster("local").setAppName("Inverted Index")
    val sc = new SparkContext(conf)
    sc.makeRDD(fileList.toSeq, maxConcurrence)
      .mapPartitionsWithIndex {
        case (partition, files) =>
          files.map(file => {
            val fs = getFileSystem()
            println(file)
            copyFile(fs, source, target, partition, file)
            fs.close()
          })
      }

    fs.close()

    //    testGetOpt()
  }

  def testGetOpt(): Unit = {
    println(getOpt(Map(), "SparkDistCp source_dir target_dir".split(" ").toList.tail))
    println(getOpt(Map(), "SparkDistCp -i source_dir target_dir".split(" ").toList.tail))
    println(getOpt(Map(), "SparkDistCp -m 10 source_dir target_dir".split(" ").toList.tail))
    println(getOpt(Map(), "SparkDistCp -i -m 10 source_dir target_dir".split(" ").toList.tail))
  }
}
