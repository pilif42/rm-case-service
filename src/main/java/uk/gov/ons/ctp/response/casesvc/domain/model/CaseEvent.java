package uk.gov.ons.ctp.response.casesvc.domain.model;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.sourceforge.cobertura.CoverageIgnore;
import uk.gov.ons.ctp.response.casesvc.representation.CategoryDTO;

/**
 * Domain model object.
 */
@CoverageIgnore
@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "caseevent", schema = "casesvc")
public class CaseEvent implements Serializable {

  private static final long serialVersionUID = 6034836141646834386L;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "caseeventseq_gen")
  @GenericGenerator(name = "caseeventseq_gen", strategy = "org.hibernate.id.enhanced.SequenceStyleGenerator",
          parameters = {
          @Parameter(name = "sequence_name", value = "casesvc.caseeventseq"),
          @Parameter(name = "increment_size", value = "1")})
  @Column(name = "caseeventpk")
  private Integer caseEventPK;

  @Column(name = "casefk")
  private Integer caseFK;

  private String description;

  @Column(name = "createdby")
  private String createdBy;

  @Column(name = "createddatetime")
  private Timestamp createdDateTime;

  @Enumerated(EnumType.STRING)
  @Column(name = "categoryfk")
  private CategoryDTO.CategoryName category;

  @Column(name = "subcategory")
  private String subCategory;
}

