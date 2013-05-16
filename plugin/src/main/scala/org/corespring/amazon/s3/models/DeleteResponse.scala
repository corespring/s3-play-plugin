package org.corespring.amazon.s3.models

case class DeleteResponse(success: Boolean, key: String, msg: String = "")
