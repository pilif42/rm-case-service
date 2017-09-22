package uk.gov.ons.ctp.response.casesvc.utility;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.ons.ctp.response.casesvc.service.CaseService;

@Slf4j
@Component
public class BespokeConfirmCallback implements RabbitTemplate.ConfirmCallback {

  public final static String DELIVERY_FAILURE_MSG = "Delivery of message to queue failed. Cause is %s.";

  @Autowired
  private CaseService caseService;

  @Override
  public void confirm(CorrelationData correlationData, boolean ack, String cause) {
    String correlationDataId = correlationData.getId();
    log.info("confirming message with ack {} - cause {} - correlationDataId {}", ack, cause, correlationDataId);
    if (!ack) {
      String errorMsg = String.format(DELIVERY_FAILURE_MSG, cause);
      log.error(errorMsg);

      caseService.rollbackForNotificationPublisher(correlationDataId);
    }
  }
}
