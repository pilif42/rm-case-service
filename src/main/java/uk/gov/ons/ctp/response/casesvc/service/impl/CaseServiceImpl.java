package uk.gov.ons.ctp.response.casesvc.service.impl;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import lombok.extern.slf4j.Slf4j;
import uk.gov.ons.ctp.common.error.CTPException;
import uk.gov.ons.ctp.common.state.StateTransitionManager;
import uk.gov.ons.ctp.common.time.DateTimeUtil;
import uk.gov.ons.ctp.response.casesvc.domain.model.Case;
import uk.gov.ons.ctp.response.casesvc.domain.model.CaseEvent;
import uk.gov.ons.ctp.response.casesvc.domain.model.CaseGroup;
import uk.gov.ons.ctp.response.casesvc.domain.model.Category;
import uk.gov.ons.ctp.response.casesvc.domain.model.Response;
import uk.gov.ons.ctp.response.casesvc.domain.repository.CaseEventRepository;
import uk.gov.ons.ctp.response.casesvc.domain.repository.CaseGroupRepository;
import uk.gov.ons.ctp.response.casesvc.domain.repository.CaseRepository;
import uk.gov.ons.ctp.response.casesvc.domain.repository.CategoryRepository;
import uk.gov.ons.ctp.response.casesvc.message.CaseNotificationPublisher;
import uk.gov.ons.ctp.response.casesvc.message.notification.CaseNotification;
import uk.gov.ons.ctp.response.casesvc.message.notification.NotificationType;
import uk.gov.ons.ctp.response.casesvc.message.sampleunitnotification.SampleUnitBase;
import uk.gov.ons.ctp.response.casesvc.message.sampleunitnotification.SampleUnitChild;
import uk.gov.ons.ctp.response.casesvc.message.sampleunitnotification.SampleUnitParent;
import uk.gov.ons.ctp.response.casesvc.representation.CaseDTO;
import uk.gov.ons.ctp.response.casesvc.representation.CaseDTO.CaseState;
import uk.gov.ons.ctp.response.casesvc.representation.CategoryDTO;
import uk.gov.ons.ctp.response.casesvc.representation.InboundChannel;
import uk.gov.ons.ctp.response.casesvc.service.ActionSvcClientService;
import uk.gov.ons.ctp.response.casesvc.service.CaseService;
import uk.gov.ons.ctp.response.casesvc.service.CollectionExerciseSvcClientService;
import uk.gov.ons.ctp.response.casesvc.service.InternetAccessCodeSvcClientService;
import uk.gov.ons.ctp.response.casesvc.utility.Constants;
import uk.gov.ons.ctp.response.collection.exercise.representation.CaseTypeDTO;
import uk.gov.ons.ctp.response.collection.exercise.representation.CollectionExerciseDTO;
import uk.gov.ons.ctp.response.sample.representation.SampleUnitDTO;
import uk.gov.ons.ctp.response.sample.representation.SampleUnitDTO.SampleUnitType;

/**
 * A CaseService implementation which encapsulates all business logic operating
 * on the Case entity model.
 */
@Service
@Slf4j
public class CaseServiceImpl implements CaseService {

  public static final String CORRELATION_DATA_ID = "%s,%s,%s";
  public static final String IAC_OVERUSE_MSG = "More than one case found to be using IAC %s";
  public static final String METHOD_CASE_DISTRIBUTOR_PROCESS_CASE = "processCase";
  public static final String MISSING_NEW_CASE_MSG = "New Case definition missing for case %s";
  public static final String WRONG_OLD_SAMPLE_UNIT_TYPE_MSG =
      "Old Case has sampleUnitType %s. It is expected to have sampleUnitType %s.";

  private static final String CASE_CREATED_EVENT_DESCRIPTION = "Case created when %s";
  private static final String METHOD_TEST_TRANSACTIONAL_BEHAVIOUR = "testTransactionalBehaviour";
  private static final String MISSING_EXISTING_CASE_MSG = "No existing Case found for caseFK %d";

  private static final int TRANSACTION_TIMEOUT = 30;

  @Autowired
  private CaseRepository caseRepo;

  @Autowired
  private CaseGroupRepository caseGroupRepo;

  @Autowired
  private StateTransitionManager<CaseDTO.CaseState, CaseDTO.CaseEvent> caseSvcStateTransitionManager;

  @Autowired
  private CaseEventRepository caseEventRepo;

  @Autowired
  private CategoryRepository categoryRepo;

  @Autowired
  private ActionSvcClientService actionSvcClientService;

  @Autowired
  private InternetAccessCodeSvcClientService internetAccessCodeSvcClientService;

  @Autowired
  private CollectionExerciseSvcClientService collectionExerciseSvcClientService;

  @Autowired
  private CaseNotificationPublisher notificationPublisher;

  @Override
  public Case findCaseByCasePK(final Integer casePK) {
    log.debug("Entering findCaseByCaseId");
    return caseRepo.findOne(casePK);
  }

  @Override
  public Case findCaseById(final UUID id) {
    log.debug("Entering findCaseById");
    return caseRepo.findById(id);
  }

  @Override
  public Case findCaseByCaseRef(final String caseRef) {
    log.debug("Entering findCaseByCaseRef");
    return caseRepo.findByCaseRef(caseRef);
  }

  @Override
  public Case findCaseByIac(final String iac) throws CTPException {
    log.debug("Entering findCaseByIac");

    List<Case> cases = caseRepo.findByIac(iac);

    Case caze = null;
    if (!CollectionUtils.isEmpty(cases)) {
      if (cases.size() != 1) {
        throw new CTPException(CTPException.Fault.SYSTEM_ERROR, String.format(IAC_OVERUSE_MSG, iac));
      }
      caze = cases.get(0);
    }

    return caze;
  }

  @Override
  public List<Case> findCasesByCaseGroupFK(final Integer caseGroupFK) {
    log.debug("Entering findCasesByCaseGroupFK");
    return caseRepo.findByCaseGroupFKOrderByCreatedDateTimeDesc(caseGroupFK);
  }

  @Override
  public List<Case> findCasesByPartyId(final UUID partyId) {
    log.debug("Entering findCasesByPartyId");
    return caseRepo.findByPartyId(partyId);
  }

  @Override
  public List<CaseEvent> findCaseEventsByCaseFK(final Integer caseFK) {
    log.debug("Entering findCaseEventsByCaseFK");
    return caseEventRepo.findByCaseFKOrderByCreatedDateTimeDesc(caseFK);
  }

  @Override
  public CaseNotification prepareCaseNotification(Case caze, CaseDTO.CaseEvent transitionEvent) {
    return new CaseNotification(caze.getId().toString(), caze.getActionPlanId().toString(),
        NotificationType.valueOf(transitionEvent.name()));
  }

  /**
   * This is where it all happens kids. After the creation of the cases from
   * sample and their subsequent distribution, this is where everything happens.
   * Anything that happens to the case from then on is thru events - created
   * here.
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = false, timeout = TRANSACTION_TIMEOUT)
  @Override
  public CaseEvent createCaseEvent(final CaseEvent caseEvent, final Case newCase) throws CTPException {
    return createCaseEvent(caseEvent, newCase, DateTimeUtil.nowUTC());
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false, timeout = TRANSACTION_TIMEOUT)
  @Override
  public CaseEvent createCaseEvent(CaseEvent caseEvent, Case newCase, Timestamp timestamp) throws CTPException {
    log.debug("Entering createCaseEvent with caseEvent {}", caseEvent);
    log.info("SPLUNK: CaseEventCreation: casePK={}, category={}, subCategory={}, createdBy={}",
        caseEvent.getCaseFK(),
        caseEvent.getCategory(),
        caseEvent.getSubCategory(),
        caseEvent.getCreatedBy());

    CaseEvent createdCaseEvent = null;

    Case targetCase = caseRepo.findOne(caseEvent.getCaseFK());
    log.debug("targetCase is {}", targetCase);
    if (targetCase != null) {
      Category category = categoryRepo.findOne(caseEvent.getCategory());

      validateCaseEventRequest(category, targetCase, newCase);

      // save the case event to db
      caseEvent.setCreatedDateTime(DateTimeUtil.nowUTC());
      createdCaseEvent = caseEventRepo.save(caseEvent);
      log.debug("createdCaseEvent is {}", createdCaseEvent);

      // do we need to record a response?
      recordCaseResponse(category, targetCase, timestamp);

      // does the event transition the case?
      effectTargetCaseStateTransition(category, targetCase);

      // should we create an ad hoc action?
      createAdHocAction(category, caseEvent);

      // should a new case be created?
      createNewCase(category, caseEvent, targetCase, newCase);
    }

    return createdCaseEvent;
  }

  @Transactional(propagation = Propagation.REQUIRED, readOnly = false, timeout = TRANSACTION_TIMEOUT)
  @Override
  public void createInitialCase(SampleUnitParent sampleUnitParent) {
    Category category = new Category();
    category.setShortDescription(String.format("Initial creation of case"));
    CaseGroup newCaseGroup = createNewCaseGroup(sampleUnitParent);
    if (sampleUnitParent.getSampleUnitChildren() != null) {
      for (SampleUnitChild sampleUnitChild : sampleUnitParent.getSampleUnitChildren().getSampleUnitchildren()) {
        Case caze = createNewCase(sampleUnitChild, newCaseGroup);
        caze.setActionPlanId(UUID.fromString(sampleUnitChild.getActionPlanId()));
        caseRepo.saveAndFlush(caze);
        createCaseCreatedEvent(caze, category);
        log.debug("New Case created: {}", caze.getId().toString());
      }
    } else {
      Case caze = createNewCase(sampleUnitParent, newCaseGroup);
      caze.setActionPlanId(UUID.fromString(sampleUnitParent.getActionPlanId()));
      caseRepo.saveAndFlush(caze);
      createCaseCreatedEvent(caze, category);
      log.debug("New Case created: {}", caze.getId().toString());
    }
  }

  /**
   * Upfront fail fast validation - if this event is going to require a new case
   * to be created, let's check the request is valid before we do something we
   * cannot rollback ie IAC disable, or Action creation.
   *
   * @param category the category details
   * @param oldCase the case the event is being created against
   * @param newCase the details provided in the event request for the new case
   * @throws CTPException if the CaseEventRequest is invalid
   */
  private void validateCaseEventRequest(Category category, Case oldCase, Case newCase) throws CTPException {
    String oldCaseSampleUnitType = oldCase.getSampleUnitType().name();
    String expectedOldCaseSampleUnitTypes = category.getOldCaseSampleUnitTypes();
    if (!compareOldCaseSampleUnitType(oldCaseSampleUnitType, expectedOldCaseSampleUnitTypes)) {
      String errorMsg = String.format(WRONG_OLD_SAMPLE_UNIT_TYPE_MSG, oldCaseSampleUnitType,
          expectedOldCaseSampleUnitTypes);
      log.error(errorMsg);
      throw new CTPException(CTPException.Fault.VALIDATION_FAILED, errorMsg);
    }

    if (category.getNewCaseSampleUnitType() != null && newCase == null) {
      String errorMsg = String.format(MISSING_NEW_CASE_MSG, oldCase.getId());
      log.error(errorMsg);
      throw new CTPException(CTPException.Fault.VALIDATION_FAILED, errorMsg);
    }

    CaseDTO.CaseEvent transitionEvent = category.getEventType();
    if (transitionEvent != null) {
      try {
        CaseState result = caseSvcStateTransitionManager.transition(oldCase.getState(), transitionEvent);
      } catch (CTPException e) {
        throw new CTPException(CTPException.Fault.VALIDATION_FAILED, e.getMessage());
      }
    }
  }

  /**
   * To compare the old case sample unit type with the expected sample unit
   * types
   *
   * @param oldCaseSampleUnitType the old case sample unit type
   * @param expectedOldCaseSampleUnitTypes a comma separated list of expected
   *          sample unit types
   * @return true if the expected types contain the old case sample unit type
   */
  private boolean compareOldCaseSampleUnitType(String oldCaseSampleUnitType, String expectedOldCaseSampleUnitTypes) {
    boolean result = false;
    if (expectedOldCaseSampleUnitTypes != null) {
      List<String> expectedTypes = Arrays.asList(expectedOldCaseSampleUnitTypes.split("\\s*,\\s*"));
      if (oldCaseSampleUnitType != null && expectedTypes.contains(oldCaseSampleUnitType)) {
        result = true;
      }
    }
    return result;
  }

  /**
   * Check to see if a new case creation is indicated by the event category and
   * if so create it
   *
   * @param category the category details of the event
   * @param caseEvent the basic event
   * @param targetCase the 'source' case the event is being created for
   * @param newCase the details for the new case (if indeed one is required)
   *          else null
   */
  private void createNewCase(Category category, CaseEvent caseEvent, Case targetCase, Case newCase) {
    if (category.getNewCaseSampleUnitType() != null) {
      // add sampleUnitType and actionplanId to newCase
      buildNewCase(category, newCase, targetCase);

      Boolean calculationRequired = category.getRecalcCollectionInstrument();
      // TODO if calculationRequired true = we need to call the Collection
      // Exercise (will only happen for CENSUS)
      if (calculationRequired == null || !calculationRequired) {
        newCase.setCollectionInstrumentId(targetCase.getCollectionInstrumentId());
      }

      createNewCaseFromEvent(caseEvent, targetCase, newCase, category);
    }
  }

  /**
   * Add required values to the new case to be created
   *
   * @param category the category details of the event
   * @param targetCase the 'source' case the event is being created for
   * @param newCase the new case to be created
   */
  private void buildNewCase(Category category, Case newCase, Case targetCase) {
    newCase.setSampleUnitType(SampleUnitType.valueOf(category.getNewCaseSampleUnitType()));

    // set case group id to the same as
    newCase.setCaseGroupId(targetCase.getCaseGroupId());

    CaseGroup caseGroup = caseGroupRepo.findOne(targetCase.getCaseGroupFK());
    CollectionExerciseDTO collectionExercise = collectionExerciseSvcClientService
        .getCollectionExercise(caseGroup.getCollectionExerciseId());

    List<CaseTypeDTO> caseTypes = collectionExercise.getCaseTypes();
    for (CaseTypeDTO caseType : caseTypes) {
      if (caseType.getSampleUnitTypeFK().equals(newCase.getSampleUnitType().name())) {
        newCase.setActionPlanId(caseType.getActionPlanId());
      }
    }
  }

  /**
   * Check to see if the event requires a response to be recorded for the case
   * and if so ... record it
   *
   * @param category the category details of the event
   * @param targetCase the 'source' case the event is being created for
   * @param timestamp timestamp the timestamp of the CaseResponse
   */
  private void recordCaseResponse(Category category, Case targetCase, Timestamp timestamp) {
    InboundChannel channel = null;
    switch (category.getCategoryName()) {
    case OFFLINE_RESPONSE_PROCESSED:
      channel = InboundChannel.OFFLINE;
      break;
    case ONLINE_QUESTIONNAIRE_RESPONSE:
      channel = InboundChannel.ONLINE;
      break;
    case PAPER_QUESTIONNAIRE_RESPONSE:
      channel = InboundChannel.PAPER;
      break;
    default:
      break;
    }
    if (channel != null) {
      Response response = Response.builder()
          .inboundChannel(channel)
          .caseFK(targetCase.getCasePK())
          .dateTime(timestamp).build();
      targetCase.getResponses().add(response);
      caseRepo.save(targetCase);
    }
  }

  /**
   * Send a request to the action service to create an ad-hoc action for the
   * event if required
   *
   * @param category the category details of the event
   * @param caseEvent the basic event
   */
  private void createAdHocAction(Category category, CaseEvent caseEvent) {
    String actionType = category.getGeneratedActionType();
    log.debug("actionType = {}", actionType);
    if (!StringUtils.isEmpty(actionType)) {
      Integer caseFk = caseEvent.getCaseFK();
      Case existingCase = caseRepo.findOne(caseFk);
      if (existingCase != null) {
        actionSvcClientService.createAndPostAction(actionType, existingCase.getId(), caseEvent.getCreatedBy());
      } else {
        log.error(String.format(MISSING_EXISTING_CASE_MSG, caseFk));
      }
    }
  }

  /**
   * Effect a state transition for the target case if the category indicates one
   * is required If a transition was made and the state changes as a result,
   * notify the action service of the state change AND if the event was type
   * DISABLED then also call the IAC service to disable/deactivate the IAC code
   * related to the target case.
   *
   * @param category the category details of the event
   * @param targetCase the 'source' case the event is being created for
   * @throws CTPException when case state transition error
   */
  private void effectTargetCaseStateTransition(Category category, Case targetCase) throws CTPException {
    CaseDTO.CaseEvent transitionEvent = category.getEventType();
    if (transitionEvent != null) {
      // case might have transitioned from actionable to inactionable prev via
      // DEACTIVATED
      // so newstate == oldstate, but always want to disable iac if event is
      // DISABLED (ie as the result
      // of an online response after a refusal) or ACCOUNT_CREATED (for BRES)
      if (transitionEvent == CaseDTO.CaseEvent.DISABLED || transitionEvent == CaseDTO.CaseEvent.ACCOUNT_CREATED) {
        internetAccessCodeSvcClientService.disableIAC(targetCase.getIac());
      }

      CaseDTO.CaseState oldState = targetCase.getState();
      CaseDTO.CaseState newState = null;
      // make the transition
      newState = caseSvcStateTransitionManager.transition(oldState, transitionEvent);

      // was a state change effected?
      if (!oldState.equals(newState)) {
        targetCase.setState(newState);
        caseRepo.saveAndFlush(targetCase);
        // TODO Should be called with correlationDataId
        notificationPublisher.sendNotification(prepareCaseNotification(targetCase, transitionEvent));
      }
    }
  }

  /**
   * Go ahead and create a new case using the new case details, associate it
   * with the target case and create the CASE_CREATED event on the new case
   *
   * @param caseEvent the basic event
   * @param targetCase the 'source' case the event is being created for
   * @param newCase the details for the new case (if indeed one is required)
   *          else null
   * @param caseEventCategory the caseEventCategory
   * @return the new case
   */
  private Case createNewCaseFromEvent(CaseEvent caseEvent, Case targetCase, Case newCase, Category caseEventCategory) {
    Case persistedCase = saveNewCase(caseEvent, targetCase, newCase);
    // NOTE the action service does not need to be notified of the creation of
    // the new case - yet
    // That will be done when the CaseDistributor wakes up and assigns an IAC to
    // the newly created case
    // ie it might be created here, but it is not yet ready for prime time
    // without its IAC!
    createCaseCreatedEvent(persistedCase, caseEventCategory);
    return persistedCase;
  }

  /**
   * Create a new case row for a replacement/new case
   *
   * @param caseEvent the event that lead to the creation of the new case
   * @param targetCase the case the caseEvent was applied to
   * @param newCase the case we have been asked to create off the back of the
   *          event
   * @return the persisted case
   */
  private Case saveNewCase(CaseEvent caseEvent, Case targetCase, Case newCase) {
    newCase.setId(UUID.randomUUID());
    newCase.setState(CaseDTO.CaseState.REPLACEMENT_INIT);
    newCase.setCreatedDateTime(DateTimeUtil.nowUTC());
    newCase.setCaseGroupFK(targetCase.getCaseGroupFK());
    newCase.setCreatedBy(caseEvent.getCreatedBy());
    newCase.setSourceCaseId(targetCase.getCasePK());
    return caseRepo.saveAndFlush(newCase);
  }

  /**
   * Create an event for a newly created case
   *
   * @param caze the case for which we want to record the event
   * @param caseEventCategory the category of the event that led to the creation
   *          of the case
   * @return the created event
   */
  private CaseEvent createCaseCreatedEvent(Case caze, Category caseEventCategory) {
    CaseEvent newCaseCaseEvent = new CaseEvent();
    newCaseCaseEvent.setCaseFK(caze.getCasePK());
    newCaseCaseEvent.setCategory(CategoryDTO.CategoryName.CASE_CREATED);
    newCaseCaseEvent.setCreatedBy(Constants.SYSTEM);
    newCaseCaseEvent.setCreatedDateTime(DateTimeUtil.nowUTC());
    newCaseCaseEvent
        .setDescription(String.format(CASE_CREATED_EVENT_DESCRIPTION, caseEventCategory.getShortDescription()));

    caseEventRepo.saveAndFlush(newCaseCaseEvent);
    return newCaseCaseEvent;
  }

  /**
   * Create the CaseGroup for the Case.
   *
   * @param caseGroupData SampleUnitParent from which to create CaseGroup.
   * @return newcaseGroup created caseGroup.
   */
  private CaseGroup createNewCaseGroup(SampleUnitParent caseGroupData) {
    CaseGroup newCaseGroup = new CaseGroup();

    newCaseGroup.setId(UUID.randomUUID());
    newCaseGroup.setPartyId(UUID.fromString(caseGroupData.getPartyId()));
    newCaseGroup.setCollectionExerciseId(UUID.fromString(caseGroupData.getCollectionExerciseId()));
    newCaseGroup.setSampleUnitRef(caseGroupData.getSampleUnitRef());
    newCaseGroup.setSampleUnitType(caseGroupData.getSampleUnitType());

    caseGroupRepo.saveAndFlush(newCaseGroup);
    log.debug("New CaseGroup created: {}", newCaseGroup.getId().toString());
    return newCaseGroup;
  }

  /**
   * Create the new Case.
   *
   * @param caseData SampleUnitParent from which to create Case.
   * @param caseGroup to which Case belongs.
   * @return newCase created Case.
   */
  private Case createNewCase(SampleUnitBase caseData, CaseGroup caseGroup) {
    Case newCase = new Case();
    newCase.setId(UUID.randomUUID());

    // values from case group
    newCase.setCaseGroupId(caseGroup.getId());
    newCase.setCaseGroupFK(caseGroup.getCaseGroupPK());

    // set case values from sampleUnit
    newCase.setSampleUnitType(SampleUnitDTO.SampleUnitType.valueOf(caseData.getSampleUnitType()));
    newCase.setPartyId(UUID.fromString(caseData.getPartyId()));
    newCase.setCollectionInstrumentId(UUID.fromString(caseData.getCollectionInstrumentId()));

    // HardCoded values
    newCase.setState(CaseState.SAMPLED_INIT);
    newCase.setCreatedDateTime(DateTimeUtil.nowUTC());
    newCase.setCreatedBy(Constants.SYSTEM);

    return newCase;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
  public void testTransactionalBehaviour() {
    String testCaseId = "551308fb-2d5a-4477-92c3-649d915834c3";
    log.info("Entering testTransactionalBehaviour with testCaseId {}", testCaseId);

    Case caze = caseRepo.findById(UUID.fromString(testCaseId));
    CaseState initialState = caze.getState();

    caze.setState(CaseState.SAMPLED_INIT);
    caseRepo.saveAndFlush(caze);
    log.debug("just saved to db");

    CaseNotification caseNotification = new CaseNotification(testCaseId, "3b136c4b-7a14-4904-9e01-13364dd7b972", null);
    notificationPublisher.sendNotification(caseNotification,
        String.format(CORRELATION_DATA_ID, METHOD_TEST_TRANSACTIONAL_BEHAVIOUR, testCaseId, initialState));
    log.info("just published to queue - last line in service");
  }

  // TODO Is this the best service for this method?
  @Override
  public void rollbackForNotificationPublisher(String correlationDataId) {
    // Pause below is required to prevent exception 'Row was updated or deleted by another transaction'
    log.info("entering rollbackForNotificationPublisher with correlationDataId {}", correlationDataId);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    }

    String[] data = correlationDataId.split(",");
    String methodName = data[0];
    log.info("methodName is {}", methodName);
    String caseId = data[1];
    log.info("caseId is {}", caseId);
    String caseStateToRevertTo = data[2];
    log.info("caseStateToRevertTo is {}", caseStateToRevertTo);

    Case caze = null;
    if (!StringUtils.isEmpty(caseId)) {
      caze = caseRepo.findById(UUID.fromString(caseId));
    }

    if (caze != null) {
      switch (methodName) {
        case METHOD_TEST_TRANSACTIONAL_BEHAVIOUR:
          caze.setState(CaseState.valueOf(caseStateToRevertTo));
          caseRepo.saveAndFlush(caze);
          log.info("case now rolledback in db");
          break;
        case METHOD_CASE_DISTRIBUTOR_PROCESS_CASE:
          caze.setState(CaseState.valueOf(caseStateToRevertTo));
          caze.setIac(null);
          caseRepo.saveAndFlush(caze);
          break;
      }
    } else {
      log.error("Unexpected situation. No case retrieved for id {}", caseId);
    }
  }
}
