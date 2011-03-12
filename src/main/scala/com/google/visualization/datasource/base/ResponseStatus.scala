// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.visualization.datasource
package base

/**
 * A response status holds three parameters:
 * 1) The response type (Ok, Warning or Error).
 * 2) The response reason type.
 * 3) A string with an error message to the user.
 *
 * @author Hillel M.
 */
object ResponseStatus {
  /**
   * The sign in message key in the <code>ResourceBundle</code>
   */
  val SIGN_IN_MESSAGE_KEY = "SIGN_IN"

  /**
   * Creates a ResponseStatus for the given DataSourceException.
   *
   * @param dse The data source exception.
   *
   * @return A response status object for the given data source exception.
   */
  def createResponseStatus(dse: DataSourceException) = {
    new ResponseStatus(StatusType.ERROR, dse.getReasonType, dse.getMessageToUser)
  }

  /**
   * Gets a modified response status in case of <code>ReasonType</code>#USER_NOT_AUTHENTICATED
   * by adding a sign in html link for the given url. If no url is provided in the
   * <code>ResponseStatus</code># no change is made.
   *
   * @param responseStatus The response status.
   *
   * @return The modified response status if modified, or else the original response status.
   */
  def getModifiedResponseStatus(responseStatus: ResponseStatus) = {
    val signInString = LocaleUtil.getLocalizedMessageFromBundle("com.google.visualization.datasource.base.ErrorMessages", SIGN_IN_MESSAGE_KEY, null)
    if (responseStatus.getReasonType == ReasonType.USER_NOT_AUTHENTICATED) {
      val msg = responseStatus.getDescription
      if (!msg.contains(" ") && (msg.startsWith("http://") || msg.startsWith("https://"))) {
        var desc = "<a target=\"_blank\" href=\"" + msg + "\">" + signInString + "</a>"
        new ResponseStatus(responseStatus.getStatusType, responseStatus.getReasonType, desc)
      } else responseStatus
    } else responseStatus
  }

}

class ResponseStatus(statusType: StatusType, reasonType: ReasonType, description: String) {

  def this(statusType: StatusType) {
    this(statusType, null, null)
  }

  /**
   * Returns the response status type.
   *
   * @return the response status type.
   */
  def getStatusType = statusType
  def getReasonType = reasonType
  def getDescription = description
}

