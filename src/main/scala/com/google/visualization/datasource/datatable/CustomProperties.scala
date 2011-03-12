package com.google.visualization.datasource
package datatable

import com.google.common.collect.Maps
import java.util.Collections
import java.util.Map

import collection.JavaConverters._

/**
 * Created by IntelliJ IDEA.
 * User: WrighKe
 * Date: 10/03/11
 * Time: 07:46
 * To change this template use File | Settings | File Templates.
 */

trait CustomProperties {
  protected var customProperties: Map[String, String] = null

  /**
   * Retrieves a custom property. Returns null if it does not exist.
   *
   * @param key The property key.
   *
   * @return The property value, or null if it does not exist.
   */
  def getCustomProperty(key: String): String = {
    if (customProperties == null) null
    else if (key == null) throw new RuntimeException("Null keys are not allowed.")
    else customProperties.get(key)
  }

  /**
   * Sets a custom property.
   *
   * @param propertyKey The property key.
   * @param propertyValue The property value.
   */
  def setCustomProperty(propertyKey: String, propertyValue: String): Unit = {
    if (customProperties == null) {
      customProperties = Maps.newHashMap[String, String]
    }
    if ((propertyKey == null) || (propertyValue == null)) {
      throw new RuntimeException("Null keys/values are not allowed.")
    }
    customProperties.put(propertyKey, propertyValue)
  }

  /**
   * Returns an immutable map of the custom properties.
   *
   * @return An immutable map of the custom properties.
   */
  def getCustomProperties: Map[String, String] = {
    if (customProperties == null) {
      return Collections.emptyMap[String, String]
    }
    return Collections.unmodifiableMap(customProperties)
  }

  def customPropsClone: Map[String, String] = {
    if (customProperties == null) {
      null
    } else {
      val result = Maps.newHashMap[String,String]
      for (entry <- customProperties.entrySet.asScala) {
        result.put(entry.getKey, entry.getValue)
      }
      result
    }    
  }
}
