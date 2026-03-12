old::
package com.ing.datadist.batch.config;

import com.ing.datadist.batch.processor.*;
import com.ing.datadist.batch.processor.OrganisationUnitEnrichmentProcessor;
import com.ing.datadist.batch.tasklet.LEDeletionApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationSearchApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationUnitSearchApiTasklet;
import com.ing.datadist.dao.LegalEntityDeletionDAO;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.dao.queries.IncapablesQueries;
import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.domain.*;
import com.ing.datadist.domain.fileinput.LegalEntityParent;
import com.ing.datadist.domain.fileinput.OrganisationUnit;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import com.ing.datadist.ecs.services.FileProviderService;
import com.ing.datadist.retention.config.DataRetentionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;
import com.ing.datadist.enums.ErrorType;
import com.ing.datadist.enums.RecordType;
import com.ing.datadist.service.FileIngestionService;

import javax.sql.DataSource;

import static com.ing.datadist.batch.config.BatchConstants.*;
import static com.ing.datadist.domain.fileinput.Incapables.*;
import static com.ing.datadist.domain.fileinput.LegalEntityChild.*;
import static com.ing.datadist.domain.fileinput.LegalEntityParent.*;
import static com.ing.datadist.domain.fileinput.OrganisationUnit.*;

@Slf4j
@Configuration
public class BatchJobConfig {

    private static final String COFACE_OPS_TABLE_INSERT_SQL =
            "INSERT INTO DD_COFACEOPS_TBL (OPS_UUID, OPS_NUMBER, ORG_UUID, ORG_NUMBER, OPS_ORGANISATIONUNITNAME, " +
                    "ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, " +
                    "ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, " +
                    "OPS_EXTID_DATEOFISSUE, OPS_EXTID_EXPIRYDATE, " +
                    "ADR_DIGITALADDR_FULLTEL, ADR_DIGITALADDR_FULLTELFGN, ADR_DIGITALADDR_EMAIL, " +
                    "FILE_ID) " +
                    "VALUES (:opsUUID, :opsExternalIdentifierVal, :orgUUID, :orgExternalIdentifierVal, :organisationUnitName, " +
                    ":address, :postalAddressStreetNm, :postalAddressHouseNum, :postalAddressHouseAdd, " +
                    ":postalAddressPostalCd, :postalAddressCityName, :postalAddressCntryCd, " +
                    ":opsExtIdDateOfIssue, :opsExtIdExpiryDate, " +
                    ":digitalAddrFullTel, :digitalAddrFullTefgn, :digitalAddrEmail, " +
                    ":fileId)";
    private final static String IP_EXTERNAL_VAL = "OPS_ExternalIdentifier_Val";
    private final static String LE_EXTERNAL_VAL = "LE_ExternalIdentifier_Val";
    private final static String OU_ORGANISATIOIN_UNITNAME = "OPS_OrganisationUnitName";
    private final static String ADDRESS = "ADDRESS";
    private final static String ADR_POSTALADDRESS_STREETNM = "Adr_PostalAddress_StreetNm";
    private final static String ADR_POSTALADDRESS_HOUSENUM = "Adr_PostalAddress_HouseNum";
    private final static String ADR_POSTALADDRESS_HOUSEDD = "Adr_PostalAddress_HouseAdd";
    private final static String ADR_POSTALADDRESS_POSTALCD = "Adr_PostalAddress_PostalCd";
    private final static String ADR_POSTALADDRESS_CITYNAME = "Adr_PostalAddress_CityName";
    private final static String ADR_POSTALADDRESS_CNTRYCD = "Adr_PostalAddress_CntryCd";
    private final static String IP_EXTID_DATEOFISSUE = "IP_ExtId_DateOfIssue";
    private final static String IP_EXTId_EXPIRYDATE = "IP_ExtId_ExpiryDate";
    private final static String ADR_DIGITALADDR_FULLTEL = "ADR_Digitaladdr_Fulltel";
    private final static String ADR_DIGITALADDR_FULLTELFGN = "Adr_Digitaladdr_Fulltelgn";
    private final static String ADR_DIGITALADDR_EMAIL = "Adr_DigitalAddr_Email";
    private final static String COFACE_OPS_BATCH_FILE_READER_NAME = "organisationUnitReader";
    private final static String COFACE_OPS_BATCH_STEP_NAME = "organisationUnitEnrichmentStep";
    private final static String COFACE_OPS_BATCH_JOB_NAME = "organisationUnitEnrichmentJob";
    private final static String COFACE_LE_BATCH_JOB_NAME = "LegalEntityEnrichmentJob";
    private final static String COFACE_LE_BATCH_FILE_READER_NAME = "organisationReader";
    private final static String COFACE_LE_BATCH_PARENT_FILE_READER_NAME = "legalEntityParentReader";
    private final static String COFACE_LE_BATCH_CHILD_FILE_READER_NAME = "legalEntityChildReader";
    String LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (" +
                    "DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, " +
                    "ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, " +
                    "ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, " +
                    "ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, " +
                    "LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, " +
                    "ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, " +
                    "ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :orgStatus, :orgName, :orgOtherName, :orgOtherNameType, " +
                    ":adrUnstructuredAddress, :adrPostalAddressStreetName, :adrPostalAddressHouseNum, " +
                    ":adrPostalAddressHouseNumAdd, :adrPostalAddressPostalCode, :adrPostalAddressCityName, " +
                    ":adrPostalAddressCountryCode, :orgLegalForm, :orgBusinessClosedownDate, " +
                    ":lELegalStatus, :asmtIdVerifyApprovalDate, :lEPreferredLanguage, " +
                    ":adrDigitalAddrFullTel, :nssoExternalIdentifier, " +
                    ":asmtVatStatus, :nssoExternalIdentifierStatus, " +
                    ":fileId)";
    String LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (" +
                    "DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :industryClassCode, :industryClassRank, :industryClassEffDate, :industryClassEndDate, :fileId)";

    @Autowired
    FileProviderService fileProviderService;
    @Autowired
    private MappingDAO mappingDAO;
    @Autowired
    private ErrorLogDAO errorLogDAO;
    @Autowired
    private TaskletCofaceOpsDomainWrapper taskletCofaceOpsDomainWrapper;
    @Autowired
    private TaskletCofaceParentDomainWrapper taskletCofaceParentDomainWrapper;
    @Autowired
    private TaskletCofaceChildDomainWrapper taskletCofaceChildDomainWrapper;
    @Autowired
    private TaskletLeDeletionDomainWrapper taskletLeDeletionDomainWrapper;
    @Autowired
    private LegalEntityDeletionDAO legalEntityDeletionDAO;
    @Autowired
    private FileIngestionService fileIngestionService;
    @Value("${ecs.bucketName}")
    private String bucketName;
    @Value("${ecs.cofaceOUFileName}")
    private String cofaceOUFileName;
    @Value("${ecs.cofaceLEParentFileName}")
    private String cofaceLEParentFileName;
    @Value("${ecs.cofaceLEChildFileName}")
    private String cofaceLEChildFileName;
    @Value("${ecs.cofaceIncapablesFileName}")
    private String cofaceIncapablesFileName;
    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
    private String cofaceLEDeleteFileName;
    @Value("${ecs.readFileFrom}")
    private String readFileFrom;
    @Value("${app.input-file}")
    private Resource resourceOUFile;
    @Value("${app.le-parent-file}")
    private Resource resourceLEParentFile;
    @Value("${app.le-child-file}")
    private Resource resourceLEChildFile;
    @Value("${app.incapables-file}")
    private Resource resourceIncapablesFile;
    @Value("${app.le-delete-file:classpath:COFACE_LE_DEL.csv}")
    private Resource resourceLEDeleteFile;

    @Bean
    @StepScope
    public FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("opsFileId");
        return new FlatFileItemReaderBuilder<OrganisationUnitDomain>()
                .name(COFACE_OPS_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceOUFileName,fileId))
                .delimited()
                .names(OrganisationUnit.LE_EXTERNAL_IDENTIFIER, OPS_EXTERNAL_IDENTIFIER_VAL, ORGANISATION_UNIT_NAME,
                        ADDRESS, UNIT_POSTAL_ADDRESS_STREET_NM, UNIT_POSTAL_ADDRESS_HOUSE_NUM,
                        UNIT_POSTAL_ADDRESS_HOUSE_ADD, UNIT_POSTAL_ADDRESS_POSTAL_CD,
                        UNIT_POSTAL_ADDRESS_CITY_NAME, UNIT_POSTAL_ADDRESS_CNTRY_CD,
                        OPS_EXT_ID_DATE_OF_ISSUE, OPS_EXT_ID_EXPIRY_DATE,
                        UNIT_DIGITAL_ADDR_FULL_TEL, UNIT_DIGITAL_ADDR_FULL_TEFGN,
                        UNIT_DIGITAL_ADDR_EMAIL)
                .fieldSetMapper(new OrganisationUnitFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public OrganisationUnitEnrichmentProcessor organisationUnitProcessor() {
        return new OrganisationUnitEnrichmentProcessor(mappingDAO, taskletCofaceOpsDomainWrapper);
    }


    @Bean
    public JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<OrganisationUnitDomainWrapper>()
                .sql(COFACE_OPS_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }


    @Bean
    public Step organisationUnitEnrichmentStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader,
                                               OrganisationUnitEnrichmentProcessor organisationUnitProcessor,
                                               JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter) {
        return new StepBuilder(COFACE_OPS_BATCH_STEP_NAME, jobRepository)
                .<OrganisationUnitDomain, OrganisationUnitDomainWrapper>chunk(100, transactionManager)
                .reader(organisationUnitReader())
                .processor(organisationUnitProcessor)
                .writer(organisationUnitWriter)
                .listener((StepExecutionListener) organisationUnitProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        organisationUnitProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(new org.springframework.batch.core.listener.SkipListenerSupport<OrganisationUnitDomain, OrganisationUnitDomainWrapper>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException) {
                            FlatFileParseException ex = (FlatFileParseException) t;
                            log.error("CSV Parsing error at line: {}, input: {}", ex.getLineNumber(), ex.getInput());

                            try {
                                errorLogDAO.logParsingError(
                                        RecordType.ORGANISATION_UNIT,
                                        "cofaceOpsData.csv",
                                        "Parsing error at line: " + ex.getLineNumber() + ", input: " + ex.getInput(),
                                        null,  // batchId
                                        ex.toString(),  // stackTrace
                                        null  // fileId
                                );
                            } catch (Exception logEx) {
                                log.error("Failed to log parsing error", logEx);
                            }
                        }
                    }
                })
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step organisationUnitSearchApiTaskletStep(JobRepository jobRepository,
                                                     PlatformTransactionManager transactionManager,
                                                     OrganisationUnitSearchApiTasklet organisationUnitSearchApiTasklet) {
        return new StepBuilder("organisationUnitSearchApiTaskletStep", jobRepository)
                .tasklet(organisationUnitSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "organisationUnitEnrichmentJob")

    public Job enrichmentJob(JobRepository jobRepository, Step organisationUnitEnrichmentStep, Step organisationUnitSearchApiTaskletStep) {
        return new JobBuilder(COFACE_OPS_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())


                .start(organisationUnitEnrichmentStep)
                .next(organisationUnitSearchApiTaskletStep)
                .build();
    }

    // Legal Entity batch config start ----------------------------------------------------------------------------------------
    @Bean
    public Step organisationSearchApiTaskletStep(JobRepository jobRepository,
                                                 PlatformTransactionManager transactionManager,
                                                 OrganisationSearchApiTasklet organisationSearchApiTasklet) {
        return new StepBuilder("organisationSearchApiTaskletStep", jobRepository)
                .tasklet(organisationSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step legalEntityParentEnrichStep(JobRepository jobRepository,
                                            PlatformTransactionManager transactionManager,
                                            FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader,
                                            LEParentEnrichmentProcessor leParentEnrichmentProcessor,
                                            JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter) {

        return new StepBuilder(COFACE_LE_BATCH_PARENT_STEP_NAME, jobRepository)
                .<LegalEntityParentDomain, LegalEntityParentDomain>chunk(100, transactionManager)
                .reader(legalEntityParentReader())
                .processor(leParentEnrichmentProcessor)
                .writer(legalEntityParentWriter)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leParentEnrichmentProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor(MappingDAO mappingDAO) {
        return new LegalEntityChildEnrichmentProcessor(mappingDAO,taskletCofaceChildDomainWrapper,errorLogDAO);
    }

    @Bean
    public Step legalEntityChildEnrichStep(JobRepository jobRepository,
                                           PlatformTransactionManager transactionManager,
                                           FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader,
                                           LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor,
                                           JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter) {

        return new StepBuilder("legalEntityChildEnrichStep", jobRepository)
                .<LegalEntityChildDomain, LegalEntityChildDomain>chunk(100, transactionManager)
                .reader(legalEntityChildReader())
                .processor(legalEntityChildEnrichmentProcessor)
                .writer(legalEntityChildWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }


    @Bean
    public LEParentEnrichmentProcessor leParentEnrichmentProcessor() {
        return new LEParentEnrichmentProcessor(mappingDAO, taskletCofaceParentDomainWrapper);
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leParentFileId");
        return new FlatFileItemReaderBuilder<LegalEntityParentDomain>()
                .name(COFACE_LE_BATCH_PARENT_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEParentFileName,fileId))
                .delimited()
                .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME,
                        ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, LE_POSTAL_ADDRESS_STREET_NAME,
                        LE_POSTAL_ADDRESS_HOUSE_NUM, LE_POSTAL_ADDRESS_HOUSE_NUM_ADD,
                        LE_POSTAL_ADDRESS_POSTAL_CODE, LE_POSTAL_ADDRESS_CITY_NAME,
                        LE_POSTAL_ADDRESS_CNTRY_CD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSEDOWN_DATE,
                        LE_LEGAL_STATUS, ASMT_ID_VERIFY_APPROVAL_DATE, LE_PREFERRED_LANGUAGE,
                        LE_DIGITAL_ADDR_FULL_TEL, NSSO_EXTERNAL_IDENTIFIER, ASMT_VAT_STATUS,
                        NSSO_EXTERNAL_IDENTIFIER_STATUS)
                .fieldSetMapper(new LegalEntityParentFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter(DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<LegalEntityParentDomain>()
                .sql(LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leChildFileId");
        return new FlatFileItemReaderBuilder<LegalEntityChildDomain>()
                .name(COFACE_LE_BATCH_CHILD_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEChildFileName,fileId))
                .delimited()
                .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_RANK,
                        INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE)
                .fieldSetMapper(new LegalEntityChildFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<LegalEntityChildDomain>()
                .sql(LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean(name = "LegalEntityEnrichmentJob")
    public Job lEEnrichmentJob(JobRepository jobRepository, Step legalEntityParentEnrichStep, Step legalEntityChildEnrichStep, Step organisationSearchApiTaskletStep) {

        return new JobBuilder(COFACE_LE_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityParentEnrichStep)
                .next(legalEntityChildEnrichStep)
                .next(organisationSearchApiTaskletStep)
                .build();
    }
    // Legal Entity batch config End ----------------------------------------------------------------------------------------


    @Bean
    @StepScope
    public FlatFileItemReader<IncapableRelationship> incapablesReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution()
                .getJobParameters()
                .getString("incapablesFileId");

        return new FlatFileItemReaderBuilder<IncapableRelationship>()
                .name(INCAPABLES_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceIncapablesFileName, fileId))
                .delimited()
                .names(INCP_GENDER, INCP_LASTNAME, INCP_FIRSTNAME, INCP_STREETNAME,
                        INCP_HOUSENUMBER, INCP_HOUSENUMBERADDITION, INCP_POSTALCODE,
                        INCP_CITYNAME, INCP_COUNTRYOFRESIDENCE, INCP_DATEOFBIRTH,
                        INCP_DATEOFDEATH, INCP_CITYOFBIRTH, INCP_COUNTRYOFBIRTH,
                        OBJECT_CODE, INAB_ENDDATE, INAB_EFFECTIVEDATE,
                        ADM_OCCUPATIONCODE, ADM_LASTNAME, ADM_FIRSTNAME,
                        ADM_STREETNAME, ADM_HOUSENUMBER, ADM_HOUSENUMBERADDITION,
                        ADM_POSTALCODE, ADM_CITYNAME, ADM_COUNTRYOFRESIDENCE,
                        ADM_RESPONSIBILITY_ENDDATE)
                .fieldSetMapper(new IncapablesFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public IncapablesEnrichmentProcessor incapablesProcessor() {
        return new IncapablesEnrichmentProcessor(mappingDAO);
    }

    @Bean
    public JdbcBatchItemWriter<IncapableRelationship> incapablesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<IncapableRelationship>()
                .sql(IncapablesQueries.INCAPABLES_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step incapablesEnrichmentStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         FlatFileItemReader<IncapableRelationship> incapablesReader,
                                         IncapablesEnrichmentProcessor incapablesProcessor,
                                         JdbcBatchItemWriter<IncapableRelationship> incapablesWriter) {
        return new StepBuilder(INCAPABLES_BATCH_STEP_NAME, jobRepository)
                .<IncapableRelationship, IncapableRelationship>chunk(100, transactionManager)
                .reader(incapablesReader)
                .processor(incapablesProcessor)
                .writer(incapablesWriter)
                .listener(incapablesProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        incapablesProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .retryLimit(3)
                .retry(Exception.class)
                .build();
    }

    @Bean(name = "IncapablesEnrichmentJob")
    public Job incapablesEnrichmentJob(JobRepository jobRepository, Step incapablesEnrichmentStep) {
        log.info("IncapablesEnrichmentJob job started");
        return new JobBuilder(INCAPABLES_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(incapablesEnrichmentStep)
                .build();
    }

    // Legal Entity Deletion batch config start ----------------------------------------------------------------------------------------

    @Bean
    @StepScope
    public FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leDeletionFileId");
        return new FlatFileItemReaderBuilder<LegalEntityDeletionDomain>()
                .name(COFACE_LE_DELETION_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEDeleteFileName, fileId))
                .delimited()
                .names(LegalEntityDeletionFieldSetMapper.LE_EXTERNAL_IDENTIFIER,
                       LegalEntityDeletionFieldSetMapper.LE_DELETION_FLAG)
                .fieldSetMapper(new LegalEntityDeletionFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor() {
        return new LEDeletionEnrichmentProcessor(mappingDAO, legalEntityDeletionDAO, taskletLeDeletionDomainWrapper, errorLogDAO);
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter(DataSource dataSource) {
        String sql = "INSERT INTO DD_LEGALENTITY_DELETE_TBL (LE_DELETION_FLAG, DD_UUID, FILE_ID, CREATED_BY, CREATED_TS) " +
                     "VALUES (:leDeletionFlag, :ddUuid, :fileId, 'DD_USER', SYSTIMESTAMP)";
        return new JdbcBatchItemWriterBuilder<LegalEntityDeletionDomain>()
                .sql(sql)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step legalEntityDeletionEnrichStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader,
                                               LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor,
                                               JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter) {
        return new StepBuilder(COFACE_LE_DELETION_BATCH_STEP_NAME, jobRepository)
                .<LegalEntityDeletionDomain, LegalEntityDeletionDomain>chunk(100, transactionManager)
                .reader(legalEntityDeletionReader())
                .processor(leDeletionEnrichmentProcessor)
                .writer(legalEntityDeletionWriter)
                .listener((StepExecutionListener) leDeletionEnrichmentProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leDeletionEnrichmentProcessor.flushRecords();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step leDeletionApiTaskletStep(JobRepository jobRepository,
                                          PlatformTransactionManager transactionManager,
                                          LEDeletionApiTasklet leDeletionApiTasklet) {
        return new StepBuilder(LE_DELETION_API_TASKLET_STEP, jobRepository)
                .tasklet(leDeletionApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "LegalEntityDeletionJob")
    public Job legalEntityDeletionJob(JobRepository jobRepository,
                                       Step legalEntityDeletionEnrichStep,
                                       Step leDeletionApiTaskletStep) {
        log.info("LegalEntityDeletionJob job started");
        return new JobBuilder(COFACE_LE_DELETION_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityDeletionEnrichStep)
                .next(leDeletionApiTaskletStep)
                .build();
    }
    // Legal Entity Deletion batch config End ----------------------------------------------------------------------------------------


    // Read File from ECS/Local-folder --------------------------------------------------------------------------------------
    private Resource getFileForLocalRun(String fileFrom, String fileName, String fileId) {
        Resource resource = null;
        log.info("DD_Data_Distributor Read File from {}, filename:{}", fileFrom, fileName);
        if (fileFrom.equalsIgnoreCase("ECS")) {
            resource = fileProviderService.getFile(bucketName, fileName);
        } else if (fileName.equalsIgnoreCase(cofaceLEChildFileName)) {
            resource = resourceLEChildFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEParentFileName)) {
            resource = resourceLEParentFile;
        } else if (fileName.equalsIgnoreCase(cofaceOUFileName)) {
            resource = resourceOUFile;
        } else if (fileName.equalsIgnoreCase(cofaceIncapablesFileName)) {
            resource = resourceIncapablesFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEDeleteFileName)) {
            resource = resourceLEDeleteFile;
        } else {
            log.error("DD_Data_Distributor Unknown File Name {}", fileName);
            return null;
        }
        log.info("DD_Data_Distributor File Downloaded Successfully...!");
        fileIngestionService.updateTotalNumberOfRecords(fileId, resource);
        return resource;
    }

    public void logDataRetentionSchedulerStatus(DataRetentionConfig dataRetentionConfig) {
        boolean isRetentionJobEnabled = dataRetentionConfig.isEnabled();
        dataRetentionConfig.logRetentionJobStatus(isRetentionJobEnabled);
    }
}
new::
 package com.ing.datadist.batch.config;

import com.ing.datadist.batch.processor.*;
import com.ing.datadist.batch.processor.OrganisationUnitEnrichmentProcessor;
import com.ing.datadist.batch.tasklet.LEDeletionApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationSearchApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationUnitSearchApiTasklet;
import com.ing.datadist.dao.LegalEntityDeletionDAO;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.dao.queries.IncapablesQueries;
import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.domain.*;
import com.ing.datadist.domain.fileinput.LegalEntityParent;
import com.ing.datadist.domain.fileinput.OrganisationUnit;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import com.ing.datadist.ecs.services.FileProviderService;
import com.ing.datadist.retention.config.DataRetentionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.SkipListenerSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;
import com.ing.datadist.enums.ErrorType;
import com.ing.datadist.enums.RecordType;
import com.ing.datadist.service.FileIngestionService;

import javax.sql.DataSource;

import static com.ing.datadist.batch.config.BatchConstants.*;
import static com.ing.datadist.domain.fileinput.Incapables.*;
import static com.ing.datadist.domain.fileinput.LegalEntityChild.*;
import static com.ing.datadist.domain.fileinput.LegalEntityParent.*;
import static com.ing.datadist.domain.fileinput.OrganisationUnit.*;

@Slf4j
@Configuration
public class BatchJobConfig {

    private static final String COFACE_OPS_TABLE_INSERT_SQL =
            "INSERT INTO DD_COFACEOPS_TBL (OPS_UUID, OPS_NUMBER, ORG_UUID, ORG_NUMBER, OPS_ORGANISATIONUNITNAME, " +
                    "ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, " +
                    "ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, " +
                    "OPS_EXTID_DATEOFISSUE, OPS_EXTID_EXPIRYDATE, " +
                    "ADR_DIGITALADDR_FULLTEL, ADR_DIGITALADDR_FULLTELFGN, ADR_DIGITALADDR_EMAIL, " +
                    "FILE_ID) " +
                    "VALUES (:opsUUID, :opsExternalIdentifierVal, :orgUUID, :orgExternalIdentifierVal, :organisationUnitName, " +
                    ":address, :postalAddressStreetNm, :postalAddressHouseNum, :postalAddressHouseAdd, " +
                    ":postalAddressPostalCd, :postalAddressCityName, :postalAddressCntryCd, " +
                    ":opsExtIdDateOfIssue, :opsExtIdExpiryDate, " +
                    ":digitalAddrFullTel, :digitalAddrFullTefgn, :digitalAddrEmail, " +
                    ":fileId)";
    private final static String IP_EXTERNAL_VAL = "OPS_ExternalIdentifier_Val";
    private final static String LE_EXTERNAL_VAL = "LE_ExternalIdentifier_Val";
    private final static String OU_ORGANISATIOIN_UNITNAME = "OPS_OrganisationUnitName";
    private final static String ADDRESS = "ADDRESS";
    private final static String ADR_POSTALADDRESS_STREETNM = "Adr_PostalAddress_StreetNm";
    private final static String ADR_POSTALADDRESS_HOUSENUM = "Adr_PostalAddress_HouseNum";
    private final static String ADR_POSTALADDRESS_HOUSEDD = "Adr_PostalAddress_HouseAdd";
    private final static String ADR_POSTALADDRESS_POSTALCD = "Adr_PostalAddress_PostalCd";
    private final static String ADR_POSTALADDRESS_CITYNAME = "Adr_PostalAddress_CityName";
    private final static String ADR_POSTALADDRESS_CNTRYCD = "Adr_PostalAddress_CntryCd";
    private final static String IP_EXTID_DATEOFISSUE = "IP_ExtId_DateOfIssue";
    private final static String IP_EXTId_EXPIRYDATE = "IP_ExtId_ExpiryDate";
    private final static String ADR_DIGITALADDR_FULLTEL = "ADR_Digitaladdr_Fulltel";
    private final static String ADR_DIGITALADDR_FULLTELFGN = "Adr_Digitaladdr_Fulltelgn";
    private final static String ADR_DIGITALADDR_EMAIL = "Adr_DigitalAddr_Email";
    private final static String COFACE_OPS_BATCH_FILE_READER_NAME = "organisationUnitReader";
    private final static String COFACE_OPS_BATCH_STEP_NAME = "organisationUnitEnrichmentStep";
    private final static String COFACE_OPS_BATCH_JOB_NAME = "organisationUnitEnrichmentJob";
    private final static String COFACE_LE_BATCH_JOB_NAME = "LegalEntityEnrichmentJob";
    private final static String COFACE_LE_BATCH_FILE_READER_NAME = "organisationReader";
    private final static String COFACE_LE_BATCH_PARENT_FILE_READER_NAME = "legalEntityParentReader";
    private final static String COFACE_LE_BATCH_CHILD_FILE_READER_NAME = "legalEntityChildReader";
    String LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (" +
                    "DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, " +
                    "ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, " +
                    "ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, " +
                    "ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, " +
                    "LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, " +
                    "ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, " +
                    "ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :orgStatus, :orgName, :orgOtherName, :orgOtherNameType, " +
                    ":adrUnstructuredAddress, :adrPostalAddressStreetName, :adrPostalAddressHouseNum, " +
                    ":adrPostalAddressHouseNumAdd, :adrPostalAddressPostalCode, :adrPostalAddressCityName, " +
                    ":adrPostalAddressCountryCode, :orgLegalForm, :orgBusinessClosedownDate, " +
                    ":lELegalStatus, :asmtIdVerifyApprovalDate, :lEPreferredLanguage, " +
                    ":adrDigitalAddrFullTel, :nssoExternalIdentifier, " +
                    ":asmtVatStatus, :nssoExternalIdentifierStatus, " +
                    ":fileId)";
    String LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (" +
                    "DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :industryClassCode, :industryClassRank, :industryClassEffDate, :industryClassEndDate, :fileId)";

    @Autowired
    FileProviderService fileProviderService;
    @Autowired
    private MappingDAO mappingDAO;
    @Autowired
    private ErrorLogDAO errorLogDAO;
    @Autowired
    private TaskletCofaceOpsDomainWrapper taskletCofaceOpsDomainWrapper;
    @Autowired
    private TaskletCofaceParentDomainWrapper taskletCofaceParentDomainWrapper;
    @Autowired
    private TaskletCofaceChildDomainWrapper taskletCofaceChildDomainWrapper;
    @Autowired
    private TaskletLeDeletionDomainWrapper taskletLeDeletionDomainWrapper;
    @Autowired
    private LegalEntityDeletionDAO legalEntityDeletionDAO;
    @Autowired
    private FileIngestionService fileIngestionService;
    @Value("${ecs.bucketName}")
    private String bucketName;
    @Value("${ecs.cofaceOUFileName}")
    private String cofaceOUFileName;
    @Value("${ecs.cofaceLEParentFileName}")
    private String cofaceLEParentFileName;
    @Value("${ecs.cofaceLEChildFileName}")
    private String cofaceLEChildFileName;
    @Value("${ecs.cofaceIncapablesFileName}")
    private String cofaceIncapablesFileName;
    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
    private String cofaceLEDeleteFileName;
    @Value("${ecs.readFileFrom}")
    private String readFileFrom;
    @Value("${app.input-file}")
    private Resource resourceOUFile;
    @Value("${app.le-parent-file}")
    private Resource resourceLEParentFile;
    @Value("${app.le-child-file}")
    private Resource resourceLEChildFile;
    @Value("${app.incapables-file}")
    private Resource resourceIncapablesFile;
    @Value("${app.le-delete-file:classpath:COFACE_LE_DEL.csv}")
    private Resource resourceLEDeleteFile;



    @Bean
    @StepScope
    public FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution().getJobParameters().getString("opsFileId");

        FlatFileItemReader<OrganisationUnitDomain> reader =
                new FlatFileItemReaderBuilder<OrganisationUnitDomain>()
                        .name("organisationUnitReader")
                        .resource(getFileForLocalRun(readFileFrom, cofaceOUFileName, fileId))
                        .delimited()
                        .names(
                                OrganisationUnit.LE_EXTERNAL_IDENTIFIER,
                                OPS_EXTERNAL_IDENTIFIER_VAL,
                                ORGANISATION_UNIT_NAME,
                                ADDRESS,
                                UNIT_POSTAL_ADDRESS_STREET_NM,
                                UNIT_POSTAL_ADDRESS_HOUSE_NUM,
                                UNIT_POSTAL_ADDRESS_HOUSE_ADD,
                                UNIT_POSTAL_ADDRESS_POSTAL_CD,
                                UNIT_POSTAL_ADDRESS_CITY_NAME,
                                UNIT_POSTAL_ADDRESS_CNTRY_CD,
                                OPS_EXT_ID_DATE_OF_ISSUE,
                                OPS_EXT_ID_EXPIRY_DATE,
                                UNIT_DIGITAL_ADDR_FULL_TEL,
                                UNIT_DIGITAL_ADDR_EMAIL
                        )
                        .fieldSetMapper(new OrganisationUnitFieldSetMapper())
                        .linesToSkip(1)
                        .build();

        reader.setRecordSeparatorPolicy(new SkipCounterLinePolicy());
        return reader;
    }
    @Bean
    public OrganisationUnitEnrichmentProcessor organisationUnitProcessor() {
        return new OrganisationUnitEnrichmentProcessor(mappingDAO, taskletCofaceOpsDomainWrapper);
    }


    @Bean
    public JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<OrganisationUnitDomainWrapper>()
                .sql(COFACE_OPS_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }


    @Bean
    public Step organisationUnitEnrichmentStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader,
                                               OrganisationUnitEnrichmentProcessor organisationUnitProcessor,
                                               JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter) {
        return new StepBuilder(COFACE_OPS_BATCH_STEP_NAME, jobRepository)
                .<OrganisationUnitDomain, OrganisationUnitDomainWrapper>chunk(100, transactionManager)
                .reader(organisationUnitReader())
                .processor(organisationUnitProcessor)
                .writer(organisationUnitWriter)
                .listener((StepExecutionListener) organisationUnitProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        organisationUnitProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(new org.springframework.batch.core.listener.SkipListenerSupport<OrganisationUnitDomain, OrganisationUnitDomainWrapper>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException) {
                            FlatFileParseException ex = (FlatFileParseException) t;
                            log.error("CSV Parsing error at line: {}, input: {}", ex.getLineNumber(), ex.getInput());

                            try {
                                errorLogDAO.logParsingError(
                                        RecordType.ORGANISATION_UNIT,
                                        "cofaceOpsData.csv",
                                        "Parsing error at line: " + ex.getLineNumber() + ", input: " + ex.getInput(),
                                        null,  // batchId
                                        ex.toString(),  // stackTrace
                                        null  // fileId
                                );
                            } catch (Exception logEx) {
                                log.error("Failed to log parsing error", logEx);
                            }
                        }
                    }
                })
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step organisationUnitSearchApiTaskletStep(JobRepository jobRepository,
                                                     PlatformTransactionManager transactionManager,
                                                     OrganisationUnitSearchApiTasklet organisationUnitSearchApiTasklet) {
        return new StepBuilder("organisationUnitSearchApiTaskletStep", jobRepository)
                .tasklet(organisationUnitSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "organisationUnitEnrichmentJob")

    public Job enrichmentJob(JobRepository jobRepository, Step organisationUnitEnrichmentStep, Step organisationUnitSearchApiTaskletStep) {
        return new JobBuilder(COFACE_OPS_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())


                .start(organisationUnitEnrichmentStep)
                .next(organisationUnitSearchApiTaskletStep)
                .build();
    }

    // Legal Entity batch config start ----------------------------------------------------------------------------------------
    @Bean
    public Step organisationSearchApiTaskletStep(JobRepository jobRepository,
                                                 PlatformTransactionManager transactionManager,
                                                 OrganisationSearchApiTasklet organisationSearchApiTasklet) {
        return new StepBuilder("organisationSearchApiTaskletStep", jobRepository)
                .tasklet(organisationSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step legalEntityParentEnrichStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader,
            LEParentEnrichmentProcessor leParentEnrichmentProcessor,
            JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter) {

        return new StepBuilder(COFACE_LE_BATCH_PARENT_STEP_NAME, jobRepository)
                .<LegalEntityParentDomain, LegalEntityParentDomain>chunk(100, transactionManager)
                .reader(legalEntityParentReader())
                .processor(leParentEnrichmentProcessor)
                .writer(legalEntityParentWriter)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leParentEnrichmentProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(new SkipListenerSupport<LegalEntityParentDomain, LegalEntityParentDomain>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException ex) {
                            log.error("LE Parent CSV parsing error at line: {}, input: {}", ex.getLineNumber(), ex.getInput());
                            try {
                                errorLogDAO.logParsingError(
                                        RecordType.ORGANISATION_PARENT,
                                        "cofaceLEParent.csv",
                                        "Parsing error at line: " + ex.getLineNumber() + ", input: " + ex.getInput(),
                                        null,
                                        ex.toString(),
                                        StepSynchronizationManager.getContext() != null
                                                ? StepSynchronizationManager.getContext().getStepExecution().getJobParameters().getString("leParentFileId")
                                                : null
                                );
                            } catch (Exception logEx) {
                                log.error("Failed to log LE Parent parsing error", logEx);
                            }
                        }
                    }
                })
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .noRetry(IncorrectTokenCountException.class)
                .build();
    }

    @Bean
    public LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor(MappingDAO mappingDAO) {
        return new LegalEntityChildEnrichmentProcessor(mappingDAO,taskletCofaceChildDomainWrapper,errorLogDAO);
    }

    @Bean
    public Step legalEntityChildEnrichStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader,
            LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor,
            JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter) {

        return new StepBuilder("legalEntityChildEnrichStep", jobRepository)
                .<LegalEntityChildDomain, LegalEntityChildDomain>chunk(100, transactionManager)
                .reader(legalEntityChildReader())
                .processor(legalEntityChildEnrichmentProcessor)
                .writer(legalEntityChildWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skip(IncorrectTokenCountException.class)   // optional but useful
                .skipLimit(Integer.MAX_VALUE)
                .listener(new org.springframework.batch.core.listener.SkipListenerSupport<LegalEntityChildDomain, LegalEntityChildDomain>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException ex) {
                            log.error("LE Child CSV parsing error at line: {}, input: {}", ex.getLineNumber(), ex.getInput());
                            try {
                                errorLogDAO.logParsingError(
                                        RecordType.ORGANISATION_CHILD,
                                        "CofaceLEChild.csv",
                                        "Parsing error at line: " + ex.getLineNumber() + ", input: " + ex.getInput(),
                                        null,
                                        ex.toString(),
                                        StepSynchronizationManager.getContext() != null
                                                ? StepSynchronizationManager.getContext().getStepExecution().getJobParameters().getString("leChildFileId")
                                                : null
                                );
                            } catch (Exception logEx) {
                                log.error("Failed to log LE Child parsing error", logEx);
                            }
                        }
                    }
                })
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .noRetry(IncorrectTokenCountException.class)
                .build();
    }


    @Bean
    public LEParentEnrichmentProcessor leParentEnrichmentProcessor() {
        return new LEParentEnrichmentProcessor(mappingDAO, taskletCofaceParentDomainWrapper);
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution().getJobParameters().getString("leParentFileId");

        FlatFileItemReader<LegalEntityParentDomain> reader =
                new FlatFileItemReaderBuilder<LegalEntityParentDomain>()
                        .name("legalEntityParentReader")
                        .resource(getFileForLocalRun(readFileFrom, cofaceLEParentFileName, fileId))
                        .delimited()
                        .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER,
                                ORG_STATUS,
                                ORG_NAME,
                                ORG_OTHER_NAME,
                                ORG_OTHER_NAME_TYPE,
                                ADR_UNSTRUCTURED_ADDRESS,
                                LE_POSTAL_ADDRESS_STREET_NAME,
                                LE_POSTAL_ADDRESS_HOUSE_NUM,
                                LE_POSTAL_ADDRESS_HOUSE_NUM_ADD,
                                LE_POSTAL_ADDRESS_POSTAL_CODE,
                                LE_POSTAL_ADDRESS_CITY_NAME,
                                LE_POSTAL_ADDRESS_CNTRY_CD,
                                ORG_LEGAL_FORM,
                                ORG_BUSINESS_CLOSEDOWN_DATE,
                                LE_LEGAL_STATUS,
                                ASMT_ID_VERIFY_APPROVAL_DATE,
                                LE_PREFERRED_LANGUAGE,
                                LE_DIGITAL_ADDR_FULL_TEL,
                                NSSO_EXTERNAL_IDENTIFIER,
                                ASMT_VAT_STATUS,
                                NSSO_EXTERNAL_IDENTIFIER_STATUS
                        )
                        .fieldSetMapper(new LegalEntityParentFieldSetMapper())
                        .linesToSkip(1)
                        .build();

        reader.setRecordSeparatorPolicy(new SkipCounterLinePolicy());
        return reader;
    }
    @Bean
    public JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter(DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<LegalEntityParentDomain>()
                .sql(LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution().getJobParameters().getString("leChildFileId");

        FlatFileItemReader<LegalEntityChildDomain> reader =
                new FlatFileItemReaderBuilder<LegalEntityChildDomain>()
                        .name("legalEntityChildReader")
                        .resource(getFileForLocalRun(readFileFrom, cofaceLEChildFileName, fileId))
                        .delimited()
                        .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER,
                                INDUSTRY_CLASS_CODE,
                                INDUSTRY_CLASS_RANK,
                                INDUSTRY_CLASS_EFF_DATE,
                                INDUSTRY_CLASS_END_DATE
                        )
                        .fieldSetMapper(new LegalEntityChildFieldSetMapper())
                        .linesToSkip(1)
                        .build();

        reader.setRecordSeparatorPolicy(new SkipCounterLinePolicy());
        return reader;
    }


    @Bean
    public JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<LegalEntityChildDomain>()
                .sql(LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean(name = "LegalEntityEnrichmentJob")
    public Job lEEnrichmentJob(JobRepository jobRepository, Step legalEntityParentEnrichStep, Step legalEntityChildEnrichStep, Step organisationSearchApiTaskletStep) {

        return new JobBuilder(COFACE_LE_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityParentEnrichStep)
                .next(legalEntityChildEnrichStep)
                .next(organisationSearchApiTaskletStep)
                .build();
    }
    // Legal Entity batch config End ----------------------------------------------------------------------------------------


    @StepScope
    @Bean
    public FlatFileItemReader<IncapableRelationship> incapablesReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution().getJobParameters().getString("incapablesFileId");

        FlatFileItemReader<IncapableRelationship> reader =
                new FlatFileItemReaderBuilder<IncapableRelationship>()
                        .name("incapablesReader")
                        .resource(getFileForLocalRun(readFileFrom, cofaceIncapablesFileName, fileId))
                        .delimited()
                        .names(
                                INCP_GENDER,
                                INCP_LASTNAME,
                                INCP_FIRSTNAME,
                                INCP_STREETNAME,
                                INCP_HOUSENUMBER,
                                INCP_HOUSENUMBERADDITION,
                                INCP_POSTALCODE,
                                INCP_CITYNAME,
                                INCP_COUNTRYOFRESIDENCE,
                                INCP_DATEOFBIRTH,
                                INCP_DATEOFDEATH,
                                INCP_CITYOFBIRTH,
                                INCP_COUNTRYOFBIRTH,
                                OBJECT_CODE,
                                INAB_ENDDATE,
                                INAB_EFFECTIVEDATE,
                                ADM_OCCUPATIONCODE,
                                ADM_LASTNAME,
                                ADM_FIRSTNAME,
                                ADM_STREETNAME,
                                ADM_HOUSENUMBER,
                                ADM_HOUSENUMBERADDITION,
                                ADM_POSTALCODE,
                                ADM_CITYNAME,
                                ADM_COUNTRYOFRESIDENCE,
                                ADM_RESPONSIBILITY_ENDDATE
                        )
                        .fieldSetMapper(new IncapablesFieldSetMapper())
                        .linesToSkip(1)
                        .build();

        reader.setRecordSeparatorPolicy(new SkipCounterLinePolicy());
        return reader;
    }

    @Bean
    public IncapablesEnrichmentProcessor incapablesProcessor() {
        return new IncapablesEnrichmentProcessor(mappingDAO);
    }

    @Bean
    public JdbcBatchItemWriter<IncapableRelationship> incapablesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<IncapableRelationship>()
                .sql(IncapablesQueries.INCAPABLES_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step incapablesEnrichmentStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         FlatFileItemReader<IncapableRelationship> incapablesReader,
                                         IncapablesEnrichmentProcessor incapablesProcessor,
                                         JdbcBatchItemWriter<IncapableRelationship> incapablesWriter) {

        return new StepBuilder(INCAPABLES_BATCH_STEP_NAME, jobRepository)
                .<IncapableRelationship, IncapableRelationship>chunk(100, transactionManager)
                .reader(incapablesReader)
                .processor(incapablesProcessor)
                .writer(incapablesWriter)
                .listener(incapablesProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        incapablesProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skip(IncorrectTokenCountException.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRetry(FlatFileParseException.class)
                .noRetry(IncorrectTokenCountException.class)
                .retry(Exception.class)
                .retryLimit(3)

                .listener(new SkipListenerSupport<IncapableRelationship, IncapableRelationship>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException ex) {
                            log.error("Incapables CSV parsing error at line {}, input={}",
                                    ex.getLineNumber(), ex.getInput());
                        }
                    }
                })
                .build();
    }

    @Bean(name = "IncapablesEnrichmentJob")
    public Job incapablesEnrichmentJob(JobRepository jobRepository, Step incapablesEnrichmentStep) {
        log.info("IncapablesEnrichmentJob job started");
        return new JobBuilder(INCAPABLES_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(incapablesEnrichmentStep)
                .build();
    }

    // Legal Entity Deletion batch config start ----------------------------------------------------------------------------------------

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution().getJobParameters().getString("leDeletionFileId");

        FlatFileItemReader<LegalEntityDeletionDomain> reader =
                new FlatFileItemReaderBuilder<LegalEntityDeletionDomain>()
                        .name("legalEntityDeletionReader")
                        .resource(getFileForLocalRun(readFileFrom, cofaceLEDeleteFileName, fileId))
                        .delimited()
                        .names(
                                LegalEntityDeletionFieldSetMapper.LE_EXTERNAL_IDENTIFIER,
                                LegalEntityDeletionFieldSetMapper.LE_DELETION_FLAG
                        )
                        .fieldSetMapper(new LegalEntityDeletionFieldSetMapper())
                        .linesToSkip(1)
                        .build();

        reader.setRecordSeparatorPolicy(new SkipCounterLinePolicy());
        return reader;
    }
    @Bean
    public LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor() {
        return new LEDeletionEnrichmentProcessor(mappingDAO, legalEntityDeletionDAO, taskletLeDeletionDomainWrapper, errorLogDAO);
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter(DataSource dataSource) {
        String sql = "INSERT INTO DD_LEGALENTITY_DELETE_TBL (LE_DELETION_FLAG, DD_UUID, FILE_ID, CREATED_BY, CREATED_TS) " +
                "VALUES (:leDeletionFlag, :ddUuid, :fileId, 'DD_USER', SYSTIMESTAMP)";
        return new JdbcBatchItemWriterBuilder<LegalEntityDeletionDomain>()
                .sql(sql)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step legalEntityDeletionEnrichStep(JobRepository jobRepository,
                                              PlatformTransactionManager transactionManager,
                                              FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader,
                                              LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor,
                                              JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter) {
        return new StepBuilder(COFACE_LE_DELETION_BATCH_STEP_NAME, jobRepository)
                .<LegalEntityDeletionDomain, LegalEntityDeletionDomain>chunk(100, transactionManager)
                .reader(legalEntityDeletionReader())
                .processor(leDeletionEnrichmentProcessor)
                .writer(legalEntityDeletionWriter)
                .listener((StepExecutionListener) leDeletionEnrichmentProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leDeletionEnrichmentProcessor.flushRecords();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step leDeletionApiTaskletStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         LEDeletionApiTasklet leDeletionApiTasklet) {
        return new StepBuilder(LE_DELETION_API_TASKLET_STEP, jobRepository)
                .tasklet(leDeletionApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "LegalEntityDeletionJob")
    public Job legalEntityDeletionJob(JobRepository jobRepository,
                                      Step legalEntityDeletionEnrichStep,
                                      Step leDeletionApiTaskletStep) {
        log.info("LegalEntityDeletionJob job started");
        return new JobBuilder(COFACE_LE_DELETION_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityDeletionEnrichStep)
                .next(leDeletionApiTaskletStep)
                .build();
    }
    // Legal Entity Deletion batch config End ----------------------------------------------------------------------------------------


    // Read File from ECS/Local-folder --------------------------------------------------------------------------------------
    private Resource getFileForLocalRun(String fileFrom, String fileName, String fileId) {
        Resource resource = null;
        log.info("DD_Data_Distributor Read File from {}, filename:{}", fileFrom, fileName);
        if (fileFrom.equalsIgnoreCase("ECS")) {
            resource = fileProviderService.getFile(bucketName, fileName);
        } else if (fileName.equalsIgnoreCase(cofaceLEChildFileName)) {
            resource = resourceLEChildFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEParentFileName)) {
            resource = resourceLEParentFile;
        } else if (fileName.equalsIgnoreCase(cofaceOUFileName)) {
            resource = resourceOUFile;
        } else if (fileName.equalsIgnoreCase(cofaceIncapablesFileName)) {
            resource = resourceIncapablesFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEDeleteFileName)) {
            resource = resourceLEDeleteFile;
        } else {
            log.error("DD_Data_Distributor Unknown File Name {}", fileName);
            return null;
        }
        log.info("DD_Data_Distributor File Downloaded Successfully...!");
        fileIngestionService.updateTotalNumberOfRecords(fileId, resource);
        return resource;
    }

    public void logDataRetentionSchedulerStatus(DataRetentionConfig dataRetentionConfig) {
        boolean isRetentionJobEnabled = dataRetentionConfig.isEnabled();
        dataRetentionConfig.logRetentionJobStatus(isRetentionJobEnabled);
    }

    public class SkipCounterLinePolicy extends DefaultRecordSeparatorPolicy {

        @Override
        public String preProcess(String record) {
            if (record == null) return null;

            String trimmed = record.trim();

            if (trimmed.isEmpty()) {
                return "";
            }

            if (trimmed.matches("^\\d{1,2}/\\d{1,2}/\\d{4}.*")) {
                return "";
            }

            String cleaned = trimmed.replace(",", "")
                    .replace("\"", "")
                    .trim();

            if (!cleaned.isEmpty() && cleaned.matches("^[0-9]+$")) {
                return "";
            }

            return record;
        }
    }}
