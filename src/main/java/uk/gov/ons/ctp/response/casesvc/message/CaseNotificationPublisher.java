package uk.gov.ons.ctp.response.casesvc.message;

import uk.gov.ons.ctp.response.casesvc.message.notification.CaseNotification;

/**
 * Service responsible for publishing case lifecycle events to notification
 * channel
 *
 */
public interface CaseNotificationPublisher {

  // TODO Get rid of this version as we need correlationDataId when pb with publishing to queue
  /**
   * To put one CaseNotification on the outbound channel caseNotificationOutbound
   * @param caseNotification the CaseNotification to put on the outbound channel
   */
  void sendNotification(CaseNotification caseNotification);

  /**
   * To put one CaseNotification on the outbound channel caseNotificationOutbound
   * @param caseNotification the CaseNotification to put on the outbound channel
   * @param correlationDataId the correlation data required to rollback in case of publish issue
   */
  void sendNotification(CaseNotification caseNotification, String correlationDataId);
}
