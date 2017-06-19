import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.util.Properties

import com.google.common.base.Preconditions
import org.apache.flume.{Context, EventDeliveryException, Transaction}
import org.apache.flume.Sink.Status
import org.apache.flume.conf.Configurable
import org.apache.flume.instrumentation.SinkCounter
import org.apache.flume.sink.AbstractSink
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, CompressionOutputStream}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by wanggang on 2017/6/15.
  */
class FlumeXJSink extends AbstractSink with Configurable {
  val logger: Logger = LoggerFactory.getLogger("FlumeXJSink")
  //基本配置变量
  var batchSize = 100
  var currentFilename: String = _
  var sinkCounter: SinkCounter = _
  //只支持本地文件和hdfs文件系统，并且最多处理一种文件系统
  val sinkTypes = Array("localFile", "HDFSFile")
  var sinkType: String = _

  //关于文件名称及文件滚动的配置变量
  var rollTimeType: String = _
  var rollTimePeriod: Int = _
  val ValidTimeTypes = Array("year", "month", "week", "day", "hour", "minute")
  val ValidMaxTimePeriods = Map("hour" -> 24, "minute" -> 59)
  //编码
  var inputCharset: String = _
  //控制文件是否应该滚动
  @volatile var shouldRotate: Boolean = false
  //flume 停止标志
  @volatile var isStopping = false

  //本地sink配置参数
  var filePath: String = _
  //hdfssink配置参数
  var HDFSPath: String = _
  var fs: FileSystem = _
  var isCompress: Boolean = _
  val serializerType = "TEXT"
  //kafkasink配置参数
  var toKafka: Boolean = _
  var topics: Array[String] = _
  var brokers: String = _
  var producer: KafkaProducer[String, String] = _

  var outputStream: BufferedOutputStream = _
  var hdfsOutputStream: FSDataOutputStream = _
  var hdfsOutputStreamForCompress: FSDataOutputStream = _
  var hdfsCompressOutputStream: CompressionOutputStream = _

  override def configure(context: Context): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    batchSize = context.getInteger("batchSize", 100)
    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0".asInstanceOf[Any])
    sinkType = context.getString("sinkType")
    Preconditions.checkNotNull(sinkType, "sinkType is required".asInstanceOf[Any])
    Preconditions.checkArgument(sinkTypes.contains(sinkType), s"sinkType $sinkType is a wrong type,please input localFile or HDFSFile.".asInstanceOf[Any])

    //本地sink配置参数
    if (sinkType == "localFile") {
      filePath = context.getString("filePath")
      Preconditions.checkNotNull(filePath, "filePath is required".asInstanceOf[Any])
      val path = new File(filePath)
      if (!path.exists()) {
        path.mkdirs()
        logger.info("configure: localFile-filePath has been created: " + filePath)
      }
    }

    //hdfssink配置参数
    if (sinkType == "HDFSFile") {
      HDFSPath = context.getString("HDFSPath")
      Preconditions.checkNotNull(HDFSPath, "HDFSPath is required".asInstanceOf[Any])
      val path = new Path(HDFSPath)
      fs = FileSystem.get(new Configuration())
      if (!fs.exists(path)) {
        fs.mkdirs(path)
        logger.info("configure: HDFSFile-HDFSPath has been created: " + path)
      }
      isCompress = context.getInteger("isCompress", 1) == 1
    }

    //kafkasink配置参数
    toKafka = context.getInteger("toKafka", 0) == 1
    if (toKafka) {
      val tmp_topics = context.getString("topics")
      Preconditions.checkNotNull(tmp_topics, "topics is required".asInstanceOf[Any])
      topics = tmp_topics.split(",")
      val brokers = context.getString("brokers")
      Preconditions.checkNotNull(brokers, "brokers is required".asInstanceOf[Any])
      val kafkaProps = new Properties()
      kafkaProps.put("bootstrap.servers", brokers)
      kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProps.put("acks", "1")
      kafkaProps.put("producer.type", "sync")
      kafkaProps.put("retries", "3")
      kafkaProps.put("linger.ms", "100")
      kafkaProps.put("auto.create.topics.enable", "true")
      val partitions = context.getInteger("partitions", 4)
      kafkaProps.put("num.partitions", partitions.toString)
      producer = new KafkaProducer[String, String](kafkaProps)
    }

    rollTimeType = context.getString("rollTimeType")
    Preconditions.checkArgument(ValidMaxTimePeriods.contains(rollTimeType), "文件切分时间类型错误".asInstanceOf[Any])
    rollTimePeriod = context.getInteger("rollTimePeriod", -1)
    Preconditions.checkArgument(rollTimePeriod > 0 && rollTimePeriod <= ValidMaxTimePeriods.getOrElse(rollTimeType, 0), "文件切分时间范围错误".asInstanceOf[Any])

    inputCharset = context.getString("inputCharset", "UTF-8").toUpperCase
    logger.info("configure: inputCharset is : " + inputCharset)

    if (sinkCounter == null) sinkCounter = new SinkCounter(getName)

  }

  override def start(): Unit = {
    logger.info("Start FlumeXJSink sink")
    sinkCounter.start()
    super.start()

    //子线程无限循环，用于文件滚动的检查
    new RotateThread().start()
  }

  class RotateThread extends Thread {
    override def run(): Unit = {
      while (!isStopping) {
        if (shouldRotate) Thread.sleep(200)
        else {
          val currentDateTime = new DateTime()
          val nextDateTimeTs = nextTimeTs(currentDateTime)
          val wTimeMs = nextDateTimeTs - currentDateTime.getMillis
          logger.info("RotateThread: Child Thread sleeps " + (wTimeMs / 1000).toString + " seconds.")
          Thread.sleep(Math.max(1L, wTimeMs - 3))
          shouldRotate = true
          logger.info("RotateThread: shouldRotate = true")
        }
      }
    }
  }

  def currentStream(): BufferedOutputStream = {
    //local-stream
    if (outputStream == null) {
      currentFilename = filePath + getCurrentFilename
      checkCurrentFilePath(currentFilename)
      outputStream = new BufferedOutputStream(new FileOutputStream(currentFilename + ".tmp"))
      logger.info("currentStream: begin file output stream for " + currentFilename + ".tmp")
    }
    outputStream
  }

  def checkCurrentFilePath(inFilename: String): Unit = {
    logger.info("checkCurrentFilePath: check localFile getAbsolutePath")
    val file = new File(inFilename)
    if (!file.exists()) {
      val fileSplit = inFilename.split(File.separator).toList
      val filePath = fileSplit.dropRight(1).mkString(File.separator)
      val path = new File(filePath)
      val flag = path.mkdirs()
      logger.info("checkCurrentFilePath: localFile getAbsolutePath is making justnow? :" + flag)
    }
    else logger.info("checkCurrentFilePath: localFile getAbsolutePath is exist")

  }

  def hdfsCurrentStream(): FSDataOutputStream = {
    //hdfs-stream
    if (hdfsOutputStream == null) {
      logger.info("hdfsOutputStream: begin to get hdfsfile output stream")
      currentFilename = HDFSPath + getCurrentFilename
      hdfsOutputStream = fs.create(new Path(currentFilename + ".tmp"))
      logger.info("hdfsOutputStream: begin hdfsfile output stream for " + currentFilename + ".tmp")
    }
    hdfsOutputStream
  }

  def hdfsCurrentCompressStream(): CompressionOutputStream = {
    //hdfs-compress-stream
    if (hdfsCompressOutputStream == null) {
      currentFilename = HDFSPath + getCurrentFilename
      hdfsOutputStreamForCompress = fs.create(new Path(currentFilename + ".lz4" + ".tmp"))
      val ccf = new CompressionCodecFactory(new Configuration())
      val cc = ccf.getCodecByName("lz4")
      hdfsCompressOutputStream = cc.createOutputStream(hdfsOutputStreamForCompress)
    }
    hdfsCompressOutputStream
  }

  override def stop(): Unit = {
    logger.info("stop: is stopping")
    isStopping = true
    sinkCounter.stop()
    super.stop()
    closeStream()
    producer.close()
  }

  override def process(): Status = {
    logger.info("process: begin process from start")
    logger.info("process: sinkType = " + sinkType)
    logger.info("process: isCompress is null? =" + isCompress)
    logger.info("process: hdfsOutputStream is null? = " + (hdfsOutputStream == null))
    logger.info("process: hdfsCompressOutputStream is null? = " + (hdfsCompressOutputStream == null))
    rotateProcess()
    try
      sinkType match { //("localFile", "HDFSFile")
        case "localFile" if outputStream == null =>
          logger.info("process: go to create currentStream()")
          currentStream()
        case "HDFSFile" if !isCompress && hdfsOutputStream == null =>
          logger.info("process: go to create hdfsCurrentStream()")
          hdfsCurrentStream()
        case "HDFSFile" if isCompress && hdfsCompressOutputStream == null =>
          logger.info("process: go to create hdfsCurrentCompressStream()")
          hdfsCurrentCompressStream()
        case _ => logger.info("process: Stream is not null,go on write")
      }
    catch {
      case e: Throwable => logger.error("process: Stream initial error", e)
        e match {
          case error1: Error => throw error1
          case _ => throw new EventDeliveryException("process: Failed to open file " + currentFilename + " while rotate", e)
        }
    }
    var transaction: Transaction = null
    var status = Status.READY

    logger.info("process: begin transaction")
    try {
      val channel = getChannel
      transaction = channel.getTransaction
      transaction.begin()
      var processCount = 0
      var runCount = 0
      if (outputStream != null) {
        logger.info("process-outputStream: outputStream begin to take event and write")
        while (runCount < batchSize) {
          val event = channel.take()
          if (event != null) {
            if (inputCharset == "UTF-8") {
              outputStream.write(event.getBody)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, new String(event.getBody))
                  producer.send(record)
                }
              }
            } else {
              val bodyString = new String(event.getBody, inputCharset)
              val body = bodyString.getBytes("UTF-8")
              outputStream.write(body)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, bodyString)
                  producer.send(record)
                }
              }
            }
            outputStream.write("\n".getBytes())
            processCount += 1
          }
          runCount += 1
        }
        outputStream.flush()
        logger.info("process-outputStream: process batchSize , flushed")
      } else if (hdfsOutputStream != null) {
        logger.info("process-hdfsOutputStream: hdfsOutputStream begin to take event and write")
        while (runCount < batchSize) {
          val event = channel.take()
          if (event != null) {
            if (inputCharset == "UTF-8") {
              hdfsOutputStream.write(event.getBody)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, new String(event.getBody))
                  producer.send(record)
                }
              }
            } else {
              val bodyString = new String(event.getBody, inputCharset)
              val body = bodyString.getBytes("UTF-8")
              hdfsOutputStream.write(body)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, bodyString)
                  producer.send(record)
                }
              }
            }
            hdfsOutputStream.write("\n".getBytes())
            processCount += 1
          }
          runCount += 1
        }
        hdfsOutputStream.flush()
        logger.info("process-hdfsOutputStream: process batchSize , flushed")
      } else if (hdfsCompressOutputStream != null) {
        logger.info("process-hdfsCompressOutputStream: hdfsCompressOutputStream begin to take event and write")
        while (runCount < batchSize) {
          val event = channel.take()
          if (event != null) {
            if (inputCharset == "UTF-8") {
              hdfsCompressOutputStream.write(event.getBody)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, new String(event.getBody))
                  producer.send(record)
                }
              }
            } else {
              val bodyString = new String(event.getBody, inputCharset)
              val body = bodyString.getBytes("UTF-8")
              hdfsCompressOutputStream.write(body)
              if (toKafka) {
                topics.foreach { t =>
                  val record = new ProducerRecord[String, String](t, bodyString)
                  producer.send(record)
                }
              }
            }
            hdfsCompressOutputStream.write("\n".getBytes())
            processCount += 1
          }
          runCount += 1
        }
        hdfsCompressOutputStream.flush()
        logger.info("process-hdfsCompressOutputStream: process batchSize , flushed")
      }
      if (processCount <= 0) {
        status = Status.BACKOFF
      }

      transaction.commit()
    } catch {
      case eio: IOException =>
        transaction.rollback()
        logger.error("process: IO error", eio)
        status = Status.BACKOFF
      case th: Throwable =>
        transaction.rollback()
        logger.error("process: process failed", th)
        status = Status.BACKOFF
        th match {
          case error1: Error => throw error1
          case _ => throw new EventDeliveryException("process: Failed to process transaction", th)
        }
    } finally {
      transaction.close()
    }
    status
  }

  def rotateProcess(): Unit = {
    if (!shouldRotate) return
    closeStream()
  }

  def closeStream(): Unit = {
    if (sinkType == "localFile") {
      if (outputStream != null) {
        try {
          outputStream.flush()
          outputStream.close()
        } catch {
          case e: Throwable => logger.error("closeStream: close stream error", e)
            e match {
              case error1: Error => throw error1
              case _ => throw new EventDeliveryException("closeStream: Failed to closeStream", e)
            }
        } finally {
          outputStream = null
          hdfsOutputStream = null
          hdfsCompressOutputStream = null
        }

        shouldRotate = false
        logger.info("closeStream: begin roll file")

        try {
          val tp = new File(currentFilename + ".tmp")
          logger.info("closeStream: temp file is" + currentFilename + ".tmp")
          val p = new File(currentFilename)
          logger.info("closeStream: file is" + currentFilename)
          tp.renameTo(p)
          logger.info("closeStream: rename temp file")
          if (p.length() == 0) {
            p.delete()
          }
        } catch {
          case e: Throwable => logger.error("closeStream: rename error", e)
            e match {
              case error1: Error => throw error1
              case _ => throw new EventDeliveryException("closeStream: Failed to rename current temp file", e)
            }
        }
      }
    } else {
      if (isCompress) {
        if (hdfsCompressOutputStream != null) {
          try {
            hdfsCompressOutputStream.flush()
            hdfsCompressOutputStream.close()
          } catch {
            case e: Throwable => logger.error("closeStream: close stream error", e)
              e match {
                case error1: Error => throw error1
                case _ => throw new EventDeliveryException("closeStream: Failed to closeStream", e)
              }
          } finally {
            outputStream = null
            hdfsOutputStream = null
            hdfsCompressOutputStream = null
          }

          shouldRotate = false
          logger.info("closeStream: begin roll file")

          try {
            val tp = new Path(currentFilename + ".lz4.tmp")
            logger.info("closeStream: temp file is" + currentFilename + ".lz4.tmp")
            val p = new Path(currentFilename + ".lz4")
            logger.info("closeStream: file is" + currentFilename)
            val fs = FileSystem.get(new Configuration())
            fs.rename(tp, p)
            logger.info("closeStream: rename temp file")
            if (fs.getFileStatus(p).getLen == 0) {
              fs.delete(p, false)
            }
          } catch {
            case e: Throwable => logger.error("closeStream: rename error", e)
              e match {
                case error1: Error => throw error1
                case _ => throw new EventDeliveryException("closeStream: Failed to rename current temp file", e)
              }
          }
        }
      } else {
        if (hdfsOutputStream != null) {
          try {
            hdfsOutputStream.flush()
            hdfsOutputStream.close()
          } catch {
            case e: Throwable => logger.error("closeStream: close stream error", e)
              e match {
                case error1: Error => throw error1
                case _ => throw new EventDeliveryException("closeStream: Failed to closeStream", e)
              }
          } finally {
            outputStream = null
            hdfsOutputStream = null
            hdfsCompressOutputStream = null
          }

          shouldRotate = false
          logger.info("closeStream: begin roll file")

          try {
            val tp = new Path(currentFilename + ".tmp")
            logger.info("closeStream: temp file is" + currentFilename + ".tmp")
            val p = new Path(currentFilename)
            logger.info("closeStream: file is" + currentFilename)
            val fs = FileSystem.get(new Configuration())
            fs.rename(tp, p)
            logger.info("closeStream: rename temp file")

            if (fs.getFileStatus(p).getLen == 0) {
              fs.delete(p, false)
            }
          } catch {
            case e: Throwable => logger.error("closeStream: rename error", e)
              e match {
                case error1: Error => throw error1
                case _ => throw new EventDeliveryException("closeStream: Failed to rename current temp file", e)
              }
          }
        }
      }
    }
  }


  def getCurrentFilename: String = {
    val d = getCurrentTime(new DateTime())
    val day = "%04d%02d%02d".format(
      d.getYear, d.getMonthOfYear, d.getDayOfMonth)

    val f = "%04d%02d%02d%02d%02d00-%s".format(
      d.getYear, d.getMonthOfYear, d.getDayOfMonth, d.getHourOfDay, d.getMinuteOfHour, System.nanoTime().toString)

    day + "/" + f
  }

  def getCurrentTime(d: DateTime): DateTime = {
    val d1 = rollTimeType match {
      case "year" => d.withDayOfYear(1).withTimeAtStartOfDay()
      case "month" => d.withMonthOfYear(1).withTimeAtStartOfDay()
      case "week" => d.withDayOfWeek(1).withTimeAtStartOfDay()
      case "day" => d.withTimeAtStartOfDay()
      case "hour" =>
        d.withHourOfDay(d.getHourOfDay / rollTimePeriod * rollTimePeriod).withMinuteOfHour(0).withSecondOfMinute(0)
      case "minute" => d.withMinuteOfHour(d.getMinuteOfHour / rollTimePeriod * rollTimePeriod).withSecondOfMinute(0)
    }
    d1
  }

  def nextTimeTs(t: DateTime): Long = {
    val d = getCurrentTime(t)
    val d1 = rollTimeType match {
      case "year" => d.plusYears(1)
      case "month" => d.plusMonths(1)
      case "week" => d.plusWeeks(1)
      case "day" => d.plusDays(1)
      case "hour" => d.plusHours(rollTimePeriod)
      case "minute" => d.plusMinutes(rollTimePeriod)
    }
    d1.getMillis
  }


}
