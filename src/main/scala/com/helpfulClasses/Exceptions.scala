package com.helpfulClasses

final case class CountException(private val message: String = "",
                                private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

final case class EqualDfException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

final case class EqualSchemasException(private val message: String = "",
                              private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

final case class EqualQueryResultException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
