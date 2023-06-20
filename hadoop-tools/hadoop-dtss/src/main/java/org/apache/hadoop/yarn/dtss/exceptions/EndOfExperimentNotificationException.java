package org.apache.hadoop.yarn.dtss.exceptions;

/**
 * This Exception is not really an error, but rather
 * a notification used to deliver the signal that
 * the experiment end time has been reached.
 */
public final class EndOfExperimentNotificationException extends Exception {
}
