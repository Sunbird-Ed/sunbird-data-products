package org.sunbird.analytics.util

import java.nio.charset.StandardCharsets
import java.util

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.JobLogger
import org.sunbird.cloud.storage.conf.AppConf
import sun.misc.BASE64Decoder

object DecryptUtil {
    
    var sunbird_encryption = ""
    
    var sunbirdEncryption = ""
    
    var encryption_key = ""
    
    val ALGORITHM = "AES"
    val ITERATIONS = 3
    var c: Cipher = null
    def initialise() : Unit = {
        try {
            sunbirdEncryption =  AppConf.getConfig("sunbird_encryption")
            sunbird_encryption = DecryptUtil.getSalt()
            val key = generateKey()
            c = Cipher.getInstance(ALGORITHM)
            c.init(Cipher.DECRYPT_MODE, key);
        } catch {
            case e: Exception => JobLogger.log(s"Error in DecryptUtil.initialise " + e.getMessage(), None, INFO)(e.getMessage)
        }
    }
    
     def getSalt() : String = {
         encryption_key = AppConf.getConfig(s"sunbird_encryption_key")
         encryption_key = replaceSpecialChars(encryption_key)
         if (StringUtils.isEmpty(encryption_key)) {
             JobLogger.log(s"Encrypt key is empty", None, INFO)(new String())
         }
         JobLogger.log(s"Encrypt key length: ${encryption_key.length}}", None, INFO)(new String())
         encryption_key
    }
    
    def replaceSpecialChars(value: String) = {
        var replacedValue = value.replace("\r","\\r")
        replacedValue = replacedValue.replace("\t", "\\t")
        replacedValue = replacedValue.replace("\n", "\\n")
        replacedValue
    }
    
    val keyValue: Array[Byte] = Array[Byte]('T', 'h', 'i', 's', 'A', 's', 'I', 'S', 'e', 'r', 'c', 'e', 'K', 't', 'e', 'y')
    def generateKey() = new SecretKeySpec(keyValue, ALGORITHM)
    
    def decryptData(data: String): String = decryptData(data, false)
    
    private def decryptData(data: String, throwExceptionOnFailure: Boolean): String =
        if (StringUtils.isBlank(data)) {
            JobLogger.log("decryptData:: data is blank", None, INFO)(new String())
            data
        } else {
            decrypt(data, throwExceptionOnFailure)
        }
    
    def decrypt(value: String, throwExceptionOnFailure: Boolean): String = {
        try {
            var dValue: String = null
            var valueToDecrypt = value.trim
            var i = 0
            while ( {
                i < ITERATIONS
            }) {
                val decordedValue = new BASE64Decoder().decodeBuffer(valueToDecrypt)
                val decValue = c.doFinal(decordedValue)
                dValue = new String(decValue, StandardCharsets.UTF_8).substring(sunbird_encryption.length)
                valueToDecrypt = dValue
                
                {
                    i += 1; i - 1
                }
            }
            return dValue
        } catch {
            case ex: Exception => {
                ex.printStackTrace()
                JobLogger.log("decrypt: Exception occurred with error message = " + ex.getMessage, None, INFO)(ex.getMessage)
                if (throwExceptionOnFailure) throw new Exception("Exception in decrypting the value")
            }
        }
        value
    }
    
}