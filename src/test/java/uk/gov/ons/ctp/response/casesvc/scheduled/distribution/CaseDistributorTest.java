package uk.gov.ons.ctp.response.casesvc.scheduled.distribution;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.ons.ctp.response.casesvc.service.impl.CaseServiceImpl.CORRELATION_DATA_ID;
import static uk.gov.ons.ctp.response.casesvc.service.impl.CaseServiceImpl.METHOD_CASE_DISTRIBUTOR_PROCESS_CASE;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import uk.gov.ons.ctp.common.FixtureHelper;
import uk.gov.ons.ctp.common.distributed.DistributedListManager;
import uk.gov.ons.ctp.common.distributed.LockingException;
import uk.gov.ons.ctp.common.error.CTPException;
import uk.gov.ons.ctp.common.state.StateTransitionManager;
import uk.gov.ons.ctp.response.casesvc.config.AppConfig;
import uk.gov.ons.ctp.response.casesvc.config.CaseDistribution;
import uk.gov.ons.ctp.response.casesvc.config.InternetAccessCodeSvc;
import uk.gov.ons.ctp.response.casesvc.domain.model.Case;
import uk.gov.ons.ctp.response.casesvc.domain.repository.CaseRepository;
import uk.gov.ons.ctp.response.casesvc.message.CaseNotificationPublisher;
import uk.gov.ons.ctp.response.casesvc.message.notification.CaseNotification;
import uk.gov.ons.ctp.response.casesvc.representation.CaseDTO;
import uk.gov.ons.ctp.response.casesvc.service.CaseService;
import uk.gov.ons.ctp.response.casesvc.service.InternetAccessCodeSvcClientService;

/**
 * Test the case distributor
 */
@RunWith(MockitoJUnitRunner.class)
public class CaseDistributorTest {

  private static final int TEN = 10;

  private static final long TEN_LONG = 10L;

  private static final String CASE_ID_1 = "1bc5d41b-0549-40b3-ba76-42f6d4cf3fd1";
  private static final String CASE_ID_2 = "2bc5d41b-0549-40b3-ba76-42f6d4cf3fd1";
  private static final String CASE_ID_3 = "3bc5d41b-0549-40b3-ba76-42f6d4cf3fd1";
  private static final String CASE_ID_4 = "4bc5d41b-0549-40b3-ba76-42f6d4cf3fd1";
  private static final String CASE_ID_5 = "6bc5d41b-0549-40b3-ba76-42f6d4cf3fd1";
  private static final String IAC = "ABCD-EFGH-IJKL-MNOP";

  private List<Case> cases;

  @Spy
  private AppConfig appConfig = new AppConfig();

  @Mock
  private PlatformTransactionManager transactionManager;  // required by transactionTemplate

  @Mock
  private TransactionTemplate transactionTemplate;

  @Mock
  private DistributedListManager<Integer> caseDistributionListManager;

  @Mock
  private InternetAccessCodeSvcClientService internetAccessCodeSvcClientService;

  @Mock
  private CaseRepository caseRepo;

  @Mock
  private CaseService caseService;

  @Mock
  private CaseNotificationPublisher notificationPublisher;

  @Mock
  private StateTransitionManager<CaseDTO.CaseState, CaseDTO.CaseEvent> caseSvcStateTransitionManager;

  @InjectMocks
  private CaseDistributor caseDistributor;

  /**
   * All of these tests require the mocked repos to respond with predictable data loaded from test fixture json files.
   *
   * @throws Exception exception thrown
   */
  @Before
  public void setUp() throws Exception {
    cases = FixtureHelper.loadClassFixtures(Case[].class);

    InternetAccessCodeSvc internetAccessCodeSvc = new InternetAccessCodeSvc();
    appConfig.setInternetAccessCodeSvc(internetAccessCodeSvc);

    CaseDistribution caseDistributionConfig = new CaseDistribution();
    caseDistributionConfig.setDelayMilliSeconds(TEN_LONG);
    caseDistributionConfig.setRetrySleepSeconds(TEN);
    caseDistributionConfig.setRetrievalMax(TEN);
    appConfig.setCaseDistribution(caseDistributionConfig);

    MockitoAnnotations.initMocks(this);
  }

  /**
   * Test where we fail at the 1st hurdle, ie we can't retrieve any cases
   *
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFailRetrievingCases() throws LockingException {
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class)))
        .thenThrow(new RuntimeException("Database access failed"));

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(0, info.getCasesFailed());
    assertEquals(0, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(0)).generateIACs(any(Integer.class));
    verify(caseRepo, times(0)).saveAndFlush(any(Case.class));
    verify(caseService, times(0)).prepareCaseNotification(any(Case.class),
            any(CaseDTO.CaseEvent.class));
    verify(notificationPublisher, times(0)).sendNotification(any(CaseNotification.class));
    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
            any(Boolean.class));
    verify(caseDistributionListManager, times(0)).unlockContainer();
  }

  /**
   * Test where we retrieve 0 case
   *
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testRetrieveZeroCase() throws LockingException {
    List<Case> cazes = new ArrayList<>();
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class))).thenReturn(cazes);

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(0, info.getCasesFailed());
    assertEquals(0, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(0)).generateIACs(any(Integer.class));
    verify(caseRepo, times(0)).saveAndFlush(any(Case.class));
    verify(caseService, times(0)).prepareCaseNotification(any(Case.class),
            any(CaseDTO.CaseEvent.class));
    verify(notificationPublisher, times(0)).sendNotification(any(CaseNotification.class));
    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
            any(Boolean.class));
    verify(caseDistributionListManager, times(1)).unlockContainer();
  }

  /**
   * Test where we retrieve cases but IAC call fails
   *
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFailIAC() throws LockingException {
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class)))
            .thenReturn(cases);
    when(internetAccessCodeSvcClientService.generateIACs(any(Integer.class))).thenThrow(
            new RuntimeException("IAC access failed"));

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(0, info.getCasesFailed());
    assertEquals(0, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(1)).generateIACs(any(Integer.class));
    verify(caseRepo, times(0)).saveAndFlush(any(Case.class));
    verify(caseService, times(0)).prepareCaseNotification(any(Case.class),
            any(CaseDTO.CaseEvent.class));
    verify(notificationPublisher, times(0)).sendNotification(any(CaseNotification.class));
    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
            any(Boolean.class));
    verify(caseDistributionListManager, times(0)).unlockContainer();
  }

  /**
   * Test where we retrieve 5 cases (all at SAMPLED_INIT or REPLACEMENT_INIT) and 5 IACs correctly.
   *
   * @throws CTPException when caseSvcStateTransitionManager.transition does
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testHappyPath() throws CTPException, LockingException {
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class)))
            .thenReturn(cases);

    List<String> iacs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      iacs.add(IAC);
    }
    when(internetAccessCodeSvcClientService.generateIACs(any(Integer.class))).thenReturn(iacs);

    when(caseSvcStateTransitionManager.transition(CaseDTO.CaseState.SAMPLED_INIT, CaseDTO.CaseEvent.ACTIVATED)).
            thenReturn(CaseDTO.CaseState.ACTIONABLE);
    when(caseSvcStateTransitionManager.transition(CaseDTO.CaseState.REPLACEMENT_INIT, CaseDTO.CaseEvent.REPLACED)).
            thenReturn(CaseDTO.CaseState.ACTIONABLE);

    CaseNotification caseNotification = new CaseNotification();
    caseNotification.setCaseId(cases.get(0).getId().toString());
    when(caseService.prepareCaseNotification(any(Case.class), any(CaseDTO.CaseEvent.class))).
            thenReturn(caseNotification);

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(0, info.getCasesFailed());
    assertEquals(5, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(1)).generateIACs(any(Integer.class));
    verify(caseRepo, times(5)).saveAndFlush(any(Case.class));
    verify(caseService, times(5)).prepareCaseNotification(any(Case.class),
            any(CaseDTO.CaseEvent.class));
    String correlationDataId1 = String.format(CORRELATION_DATA_ID, METHOD_CASE_DISTRIBUTOR_PROCESS_CASE, CASE_ID_1,
        CaseDTO.CaseState.SAMPLED_INIT);
    verify(notificationPublisher, times(1)).sendNotification(any(CaseNotification.class),
        eq(correlationDataId1));
    String correlationDataId2 = String.format(CORRELATION_DATA_ID, METHOD_CASE_DISTRIBUTOR_PROCESS_CASE, CASE_ID_2,
        CaseDTO.CaseState.SAMPLED_INIT);
    verify(notificationPublisher, times(1)).sendNotification(any(CaseNotification.class),
        eq(correlationDataId2));
    String correlationDataId3 = String.format(CORRELATION_DATA_ID, METHOD_CASE_DISTRIBUTOR_PROCESS_CASE, CASE_ID_3,
        CaseDTO.CaseState.REPLACEMENT_INIT);
    verify(notificationPublisher, times(1)).sendNotification(any(CaseNotification.class),
        eq(correlationDataId3));
    String correlationDataId4 = String.format(CORRELATION_DATA_ID, METHOD_CASE_DISTRIBUTOR_PROCESS_CASE, CASE_ID_4,
        CaseDTO.CaseState.REPLACEMENT_INIT);
    verify(notificationPublisher, times(1)).sendNotification(any(CaseNotification.class),
        eq(correlationDataId4));
    String correlationDataId5 = String.format(CORRELATION_DATA_ID, METHOD_CASE_DISTRIBUTOR_PROCESS_CASE, CASE_ID_5,
        CaseDTO.CaseState.REPLACEMENT_INIT);
    verify(notificationPublisher, times(1)).sendNotification(any(CaseNotification.class),
        eq(correlationDataId5));

    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
            any(Boolean.class));
    verify(caseDistributionListManager, times(0)).unlockContainer();
  }

  /**
   * Test where we retrieve 6 cases but only 4 IACs correctly.
   *
   * 5 Cases have a correct state (SAMPLED_INIT or REPLACEMENT_INIT). 1 case has an incorrect state (ACTIONABLE).
   *
   * @throws CTPException when caseSvcStateTransitionManager.transition does
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testWeDontRetrieveEnoughIACs() throws CTPException, LockingException {
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class)))
            .thenReturn(cases);

    List<String> iacs = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      iacs.add(IAC);
    }
    when(internetAccessCodeSvcClientService.generateIACs(any(Integer.class))).thenReturn(iacs);

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(0, info.getCasesFailed());
    assertEquals(0, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(1)).generateIACs(any(Integer.class));
    verify(caseRepo, times(0)).saveAndFlush(any(Case.class));
    verify(caseService, times(0)).prepareCaseNotification(any(Case.class),
            any(CaseDTO.CaseEvent.class));
    verify(notificationPublisher, times(0)).sendNotification(any(CaseNotification.class));
    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
            any(Boolean.class));
    verify(caseDistributionListManager, times(0)).unlockContainer();
  }

  /**
   * Test where we retrieve 5 cases (all at SAMPLED_INIT or REPLACEMENT_INIT) and 5 IACs correctly. But, on processing,
   * 1 case out of 5 is throwing an exception.
   *
   * @throws CTPException when caseSvcStateTransitionManager.transition does
   * @throws LockingException when caseDistributionListManager does
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDBExceptionThrownDuringProcessing() throws CTPException, LockingException {
    when(caseRepo.findByStateInAndCasePKNotIn(any(List.class), any(List.class), any(Pageable.class)))
        .thenReturn(cases);

    List<String> iacs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      iacs.add(IAC);
    }
    when(internetAccessCodeSvcClientService.generateIACs(any(Integer.class))).thenReturn(iacs);

    when(caseSvcStateTransitionManager.transition(CaseDTO.CaseState.SAMPLED_INIT, CaseDTO.CaseEvent.ACTIVATED)).
        thenReturn(CaseDTO.CaseState.ACTIONABLE);
    when(caseSvcStateTransitionManager.transition(CaseDTO.CaseState.REPLACEMENT_INIT, CaseDTO.CaseEvent.REPLACED)).
        thenReturn(CaseDTO.CaseState.ACTIONABLE);

    when(caseRepo.saveAndFlush(any(Case.class))).thenThrow(new RuntimeException("The DB is KO at the moment."));

    CaseDistributionInfo info = caseDistributor.distribute();
    assertEquals(5, info.getCasesFailed());
    assertEquals(0, info.getCasesSucceeded());

    verify(internetAccessCodeSvcClientService, times(1)).generateIACs(any(Integer.class));
    verify(caseRepo, times(5)).saveAndFlush(any(Case.class));
    verify(caseService, times(0)).prepareCaseNotification(any(Case.class),
        any(CaseDTO.CaseEvent.class));
    verify(notificationPublisher, times(0)).sendNotification(any(CaseNotification.class));
    verify(caseDistributionListManager, times(1)).deleteList(any(String.class),
        any(Boolean.class));
    verify(caseDistributionListManager, times(0)).unlockContainer();
  }
}
