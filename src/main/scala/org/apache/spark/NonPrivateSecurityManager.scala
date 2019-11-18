package org.apache.spark


class NonPrivateSecurityManager(sparkConf: SparkConf, override val ioEncryptionKey: Option[Array[Byte]] = None)
  extends SecurityManager(sparkConf, ioEncryptionKey)