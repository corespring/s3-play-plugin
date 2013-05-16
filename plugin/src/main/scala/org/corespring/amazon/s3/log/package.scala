package org.corespring.amazon.s3

import play.api.LoggerLike

package object log {

  private[s3] def Logger : LoggerLike = play.api.Logger("s3-play-plugin")

}
