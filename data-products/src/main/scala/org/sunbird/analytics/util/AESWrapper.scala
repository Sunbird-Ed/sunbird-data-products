package org.sunbird.analytics.util

import org.ekstep.analytics.framework.conf.AppConf

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec


object AESWrapper {
  private val ALGO = "AES" // Default uses ECB PKCS5Padding

  @throws[Exception]
  def encrypt(Data: String, secret: Option[String]): String = {
    val key = generateKey(secret.getOrElse(AppConf.getConfig("uci_encryption_secret")))
    val c = Cipher.getInstance(ALGO)
    c.init(Cipher.ENCRYPT_MODE, key)
    val encVal = c.doFinal(Data.getBytes)
    Base64.getEncoder.encodeToString(encVal)
  }

  def decrypt(strToDecrypt: String, secret: Option[String]): String = {
    try {
      val key = generateKey(secret.getOrElse(AppConf.getConfig("uci_encryption_secret")))
      val cipher = Cipher.getInstance(ALGO)
      cipher.init(Cipher.DECRYPT_MODE, key)
      new String(cipher.doFinal(Base64.getDecoder.decode(strToDecrypt)))
    } catch {
      case e: Exception =>
        System.out.println("Error while decrypting: " + e.toString)
        null
    }
  }

  @throws[Exception]
  private def generateKey(secret: String) = {
    val decoded = Base64.getDecoder.decode(secret.getBytes)
    new SecretKeySpec(decoded, ALGO)
  }

  def decodeKey(str: String): String = {
    val decoded = Base64.getDecoder.decode(str.getBytes)
    new String(decoded)
  }

  def encodeKey(str: String): String = {
    val encoded = Base64.getEncoder.encode(str.getBytes)
    new String(encoded)
  }
}