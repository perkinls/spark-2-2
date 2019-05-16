/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{List => JList}
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.util.Utils

/**
  *
  * 解析和封装来自spark - submit脚本的参数。
  * env参数用于测试。
  */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser {
  var master: String = null
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var packagesExclusions: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: SparkSubmitAction = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null

  // Standalone cluster mode only   仅仅用于Standalone独立模式的参数
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = true // used internally    内部使用

  /**
    * 当前定义的默认文件中的默认属性。
    */
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()

    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach { case (k, v) =>
        defaultProperties(k) = v
      }
      // 属性文件可能包含敏感信息，因此在打印前进行编辑
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          SparkSubmit.printStream.println(s"Adding default property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  //从命令行参数设置参数
  try {
    parse(args.asJava)
  } catch {
    case e: IllegalArgumentException => SparkSubmit.printErrorAndExit(e.getMessage())
  }
  //从属性文件填充“sparkProperties”映射
  mergeDefaultSparkProperties()

  // 从sparkProperties中移除key不是以 "spark." 开头的变量
  ignoreNonSparkProperties()
  //使用`sparkProperties`映射和env变量来填充任何缺少的参数
  loadEnvironmentArguments()

  //针对不同的action进行有效参数校验
  validateArguments()

  /**
    * 将默认属性文件中的值与通过- conf指定的值合并。当它被调用时，
    * “sparkProperties”已经从后者填充了configs。
    */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
    * 从sparkProperties中移除key不是以 "spark." 开头的变量
    */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }

  /**
    * 从环境变量，Spark properties中加载参数
    */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull
    files = Option(files).orElse(sparkProperties.get("spark.files")).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    deployMode = Option(deployMode)
      .orElse(sparkProperties.get("spark.submit.deployMode"))
      .orElse(env.get("DEPLOY_MODE"))
      .orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)
    queue = Option(queue).orElse(sparkProperties.get("spark.yarn.queue")).orNull
    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
              "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    // 全局的违约。为了避免混淆的行为，这些应该保持最小。
    master = Option(master).getOrElse("local[*]")

    // 在Spark模式下，app应用程序的名称可以通过SPARK_YARN_APP_NAME设置(详情请看 SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // 如果没有指定，那么就从主类中设置名称
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    // 除非另有说明，否则应提交行动
    action = Option(action).getOrElse(SUBMIT)
  }

  /**
    * 确保所需要的字段存在。只有在加载所有默认值时才调用此功能。
    */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
    }
  }

  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && SparkSubmit.isUserJar(primaryResource)) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }

    if (proxyUser != null && principal != null) {
      SparkSubmit.printErrorAndExit("Only one of --proxy-user or --principal can be provided.")
    }
  }

  private def validateKillArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Killing submissions is only supported in standalone or Mesos mode!")
    }
    if (submissionToKill == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Requesting submission statuses is only supported in standalone or Mesos mode!")
    }
    if (submissionToRequestStatusFor == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to request status for.")
    }
  }

  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString: String = {
    s"""Parsed arguments:
       |  master                  $master
       |  deployMode              $deployMode
       |  executorMemory          $executorMemory
       |  executorCores           $executorCores
       |  totalExecutorCores      $totalExecutorCores
       |  propertiesFile          $propertiesFile
       |  driverMemory            $driverMemory
       |  driverCores             $driverCores
       |  driverExtraClassPath    $driverExtraClassPath
       |  driverExtraLibraryPath  $driverExtraLibraryPath
       |  driverExtraJavaOptions  $driverExtraJavaOptions
       |  supervise               $supervise
       |  queue                   $queue
       |  numExecutors            $numExecutors
       |  files                   $files
       |  pyFiles                 $pyFiles
       |  archives                $archives
       |  mainClass               $mainClass
       |  primaryResource         $primaryResource
       |  name                    $name
       |  childArgs               [${childArgs.mkString(" ")}]
       |  jars                    $jars
       |  packages                $packages
       |  packagesExclusions      $packagesExclusions
       |  repositories            $repositories
       |  verbose                 $verbose
       |
    |Spark properties used, including those specified through
       | --conf and those from the properties file $propertiesFile:
       |${Utils.redact(sparkProperties).mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /**
    * 通过解析用户选项来进行属性填充值。
    */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME => name = value

      case MASTER => master = value

      case CLASS => mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      case DRIVER_JAVA_OPTIONS =>
        driverExtraJavaOptions = value

      case DRIVER_LIBRARY_PATH =>
        driverExtraLibraryPath = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case KILL_SUBMISSION =>
        submissionToKill = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $KILL.")
        }
        action = KILL

      case STATUS =>
        submissionToRequestStatusFor = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $REQUEST_STATUS.")
        }
        action = REQUEST_STATUS

      case SUPERVISE =>
        supervise = true

      case QUEUE =>
        queue = value

      case FILES =>
        files = Utils.resolveURIs(value)

      case PY_FILES =>
        pyFiles = Utils.resolveURIs(value)

      case ARCHIVES =>
        archives = Utils.resolveURIs(value)

      case JARS =>
        jars = Utils.resolveURIs(value)

      case PACKAGES =>
        packages = value

      case PACKAGES_EXCLUDE =>
        packagesExclusions = value

      case REPOSITORIES =>
        repositories = value

      case CONF =>
        val (confName, confValue) = SparkSubmit.parseSparkConfProperty(value)
        sparkProperties(confName) = confValue

      case PROXY_USER =>
        proxyUser = value

      case PRINCIPAL =>
        principal = value

      case KEYTAB =>
        keytab = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        SparkSubmit.printVersionAndExit()

      case USAGE_ERROR =>
        printUsageAndExit(1)

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
    * 处理未识别的命令行选项。
    *
    * 第一个未被识别的选项被视为“主要资源”。其他的都被当作应用程序参数来处理。
    */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      SparkSubmit.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    primaryResource =
      if (!SparkSubmit.isShell(opt) && !SparkSubmit.isInternal(opt)) {
        Utils.resolveURI(opt).toString
      } else {
        opt
      }
    isPython = SparkSubmit.isPython(opt)
    isR = SparkSubmit.isR(opt)
    false
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra.asScala
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    // scalastyle:off println
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.get("_SPARK_CMD_USAGE").getOrElse(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]
        |Usage: spark-submit run-example [options] example-class [example args]""".stripMargin)
    outStream.println(command)

    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
    outStream.println(
      s"""
         |Options:
         |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
         |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
         |                              on one of the worker machines inside the cluster ("cluster")
         |                              (Default: client).
         |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
         |  --name NAME                 A name of your application.
         |  --jars JARS                 Comma-separated list of local jars to include on the driver
         |                              and executor classpaths.
         |  --packages                  Comma-separated list of maven coordinates of jars to include
         |                              on the driver and executor classpaths. Will search the local
         |                              maven repo, then maven central and any additional remote
         |                              repositories given by --repositories. The format for the
         |                              coordinates should be groupId:artifactId:version.
         |  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
         |                              resolving the dependencies provided in --packages to avoid
         |                              dependency conflicts.
         |  --repositories              Comma-separated list of additional remote repositories to
         |                              search for the maven coordinates given with --packages.
         |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
         |                              on the PYTHONPATH for Python apps.
         |  --files FILES               Comma-separated list of files to be placed in the working
         |                              directory of each executor. File paths of these files
         |                              in executors can be accessed via SparkFiles.get(fileName).
         |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
         |  --properties-file FILE      Path to a file from which to load extra properties. If not
         |                              specified, this will look for conf/spark-defaults.conf.
         |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).
         |  --driver-java-options       Extra Java options to pass to the driver.
         |  --driver-library-path       Extra library path entries to pass to the driver.
         |  --driver-class-path         Extra class path entries to pass to the driver. Note that
         |                              jars added with --jars are automatically included in the
         |                              classpath.
         |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
         |
        |  --proxy-user NAME           User to impersonate when submitting the application.
         |                              This argument does not work with --principal / --keytab.
         |
        |  --help, -h                  Show this help message and exit.
         |  --verbose, -v               Print additional debug output.
         |  --version,                  Print the version of current Spark.
         |
        | Spark standalone with cluster deploy mode only:
         |  --driver-cores NUM          Cores for driver (Default: 1).
         |
        | Spark standalone or Mesos with cluster deploy mode only:
         |  --supervise                 If given, restarts the driver on failure.
         |  --kill SUBMISSION_ID        If given, kills the driver specified.
         |  --status SUBMISSION_ID      If given, requests the status of the driver specified.
         |
        | Spark standalone and Mesos only:
         |  --total-executor-cores NUM  Total cores for all executors.
         |
        | Spark standalone and YARN only:
         |  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
         |                              or all available cores on the worker in standalone mode)
         |
        | YARN-only:
         |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
         |                              (Default: 1).
         |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
         |  --num-executors NUM         Number of executors to launch (Default: 2).
         |                              If dynamic allocation is enabled, the initial number of
         |                              executors will be at least NUM.
         |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
         |                              working directory of each executor.
         |  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
         |                              secure HDFS.
         |  --keytab KEYTAB             The full path to the file that contains the keytab for the
         |                              principal specified above. This keytab will be copied to
         |                              the node running the Application Master via the Secure
         |                              Distributed Cache, for renewing the login tickets and the
         |                              delegation tokens periodically.
      """.stripMargin
    )

    if (SparkSubmit.isSqlShell(mainClass)) {
      outStream.println("CLI options:")
      outStream.println(getSqlShellOptions())
    }
    // scalastyle:on println

    SparkSubmit.exitFn(exitCode)
  }

  /**
    *
    * 使用"--help"选项运行Spark SQL CLI主类并捕获它的输出。然后过滤结果以去除多余的行。
    *
    * 由于CLI将调用“system . exit()”我们安装了一个安全管理器来防止这个调用工作，然后恢复原来的那个调用。
    */
  private def getSqlShellOptions(): String = {
    val currentOut = System.out
    val currentErr = System.err
    val currentSm = System.getSecurityManager()
    try {
      val out = new ByteArrayOutputStream()
      val stream = new PrintStream(out)
      System.setOut(stream)
      System.setErr(stream)

      val sm = new SecurityManager() {
        override def checkExit(status: Int): Unit = {
          throw new SecurityException()
        }

        override def checkPermission(perm: java.security.Permission): Unit = {}
      }
      System.setSecurityManager(sm)

      try {
        Utils.classForName(mainClass).getMethod("main", classOf[Array[String]])
          .invoke(null, Array(HELP))
      } catch {
        case e: InvocationTargetException =>
          // Ignore SecurityException, since we throw it above.
          if (!e.getCause().isInstanceOf[SecurityException]) {
            throw e
          }
      }

      stream.flush()

      // Get the output and discard any unnecessary lines from it.
      Source.fromString(new String(out.toByteArray(), StandardCharsets.UTF_8)).getLines
        .filter { line =>
          !line.startsWith("log4j") && !line.startsWith("usage")
        }
        .mkString("\n")
    } finally {
      System.setSecurityManager(currentSm)
      System.setOut(currentOut)
      System.setErr(currentErr)
    }
  }
}
