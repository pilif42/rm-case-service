package uk.gov.ons.ctp.response.iac.representation;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Domain model object for representation of the IAC data in the context of its associated case.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class InternetAccessCodeCaseContextDTO {

  @NotNull
  private UUID  caseId;

  @NotNull
  private String caseRef;

  @NotNull
  private String iac;

  private Boolean active;

  @NotNull
  private String questionSet;

  private Timestamp lastUsedDateTime;
}
