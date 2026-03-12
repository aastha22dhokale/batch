this is my code directory ok:
DataDistribution
.idea
OilFiles
D_20260311162004_OrganisationUnit_V1_DIDDCDBE.json
D_20260311162005_Organisation_V1_DIDDCDBE.json
F_20260311162900_Individual_V1_DIDDCDBE.json
F_20260311163200_Individual_V1_DIDDCDBE.json
F_20260311163200_InvolvedPartyInvolvedPartyRelationship_V1_DIDDCDBE.json
src
main
java
datadist
api
exception
DataDistributionApiException
model
AssessmentResponseV5
AssessmentsResponseV5
AssociatedPartyRelationship
CreateAssessmentRequest
CreateAssessmentResponse
CreateAssociatedPartyRelationshipRequest
CreateAssociatedPartyRelationshipResponse
CreateDigitalAddressV5Request
CreateDigitalAddressV5Response
CreateIndividualRequest
CreateInternalIdentifierRequest
CreateInvolvedPartyRequestV5
CreateInvolvedPartyResponseV5
CreateOccupationRequest
CreateOccupationResponse
CreateOrganisationIndustryClassificationRequest
CreateOrganisationIndustryClassificationResponse
CreateOrganisationNameRequest
CreateOrganisationNameResponse
CreateOrganisationRequest
CreateOrganisationUnitNameRequest
CreateOrganisationUnitRequest
CreateOrganisationUnitResponse
CreatePartyMarkingRequest
CreatePartyMarkingResponse
CreatePostalAddressRequest
DigitalAddressResponse
ErrorResponse
ExternalIdentifiersResponseV5
GetGranteesRequest
GetGranteesResponse
GetInvolvedPartyResponseV6
GroupResponse
IdentifyInvolvedPartiesRequest
IdentifyInvolvedPartiesResponse
IndividualNameRequest
IndividualNameResponse
industryClassificationResponseV5
IndustryClassificationsResponseV5
InnerErrorResponse
InternalIdentifierResponse
InvolvedPartiesIndividualResponse
InvolvedPartiesLinkHrefResponse
InvolvedPartiesLinkSelfResponse
InvolvedPartiesOrganisationResponse
InvolvedPartiesOrganisationUnitResponse
InvolvedPartyExternalIdentifierResponseV5
InvolvedPartyRelationships
ManagingEntityResponse
OrganisationHierarchyRelationshipsResponse
OrganisationNameResponse
OrganisationUnit
OrganisationUnitNameResponse
OrganisationUnitOrganisationRelationshipRequest
OrganisationUnitOrganisationRelationshipRequestV1
OrganisationUnitOrganisationRelationshipResponse
PatchIndividualRequest
PatchIndividualResponse
PostalAddressResponse
RelationshipGrantee
RelationshipGrantor
SearchChitizenshipResponseV1
SearchIndividualNameResponseV1
SearchIndividualResponseV1
SearchInvolvedPartiesRequestV1
SearchInvolvedPartiesResponseV1
SearchInvolvedPartyByInternalIdentifierRequestV1
SearchOrganisationNameResponseV1
SearchOrganisationResponseV1
SearchOrganisationUnitAddressResponseV1
SearchOrganisationUnitAddressResponseV2
SearchOrganisationUnitDigitalAddressesV1
SearchOrganisationUnitDigitalAddressesV2
SearchOrganisationUnitExternalIdentifierResponseV1
SearchOrganisationUnitExternalIdentifierResponseV2
SearchOrganisationUnitInternalIdentifierResponseV1
SearchOrganisationUnitInternalIdentifierResponseV2
SearchOrganisationUnitInvolvedPartyTypeResponseV2
SearchOrganisationUnitListResponseV2
SearchOrganisationUnitNameResponseV1
SearchOrganisationUnitNameResponseV2
SearchOrganisationUnitResponseV1
SearchOrganisationUnitResponseV2
SoftClosePostalAddressRequest
UpdateAssessmentRequest
UpdateAssessmentResponse
UpdateAssociatedPartyRelationshipRequest
UpdateAssociatedPartyRelationshipResponse
UpdateDigitalAddressV5Request
UpdateDigitalAddressV5Response
UpdateExternalIdentifierInvolvedPartyRequest
UpdateExternalIdentifierInvolvedPartyResponse
UpdateInvolvedPartyIndividualNamesResponseV5
UpdateInvolvedPartyIndividualRequestV5
UpdateInvolvedPartyIndividualResponseV5
UpdateInvolvedPartyInternalIdentifierResponseV5
UpdateOrganisationIndustryClassificationRequest
UpdateOrganisationIndustryClassificationResponse
UpdateOrganisationNameRequest
UpdateOrganisationNameResponse
UpdateOrganisationRequest
UpdateOrganisationUnitNameV5Request
UpdateOrganisationUnitNameV5Response
UpdatePostalAddressRequestV5
UpdatePostalAddressV5Request
UpdatePostalAddressV5Response
service
AddressService
AssociatedPartyRelationshipService
DeleteOrganisationHierarchyService
DigitalAddrService
IdentifyInvolvedPartySearchService
IndividualOnePamRepository
IndividualService.java
IndustryClassificationService
InvolvedPartyOnePamRepository
InvolvedPartySearchService
OccupationService
OrganisationAssessmentService
OrganisationNameService
OrganisationOnePamRepository
OrganisationService
OrganisationUnitNameService
OrganisationUnitOnePamRepository
OrganisationUnitService
PartyMarkingService
PhoenixDiscoveryService
SoftClosePostalAddressService
UpdateOrganisationChildService
UpdateOrganisationParentService
UpdateOrganisationUnitService
utils
HeartbeatController
batch
config
BatchConstants
BatchJobConfig
IncapablesFieldSetMapper
LegalEntityChildFieldSetMapper
LegalEntityDeletionFieldSetMapper
LegalEntityParentFieldSetMapper
OrganisationUnitFieldSetMapper
listener
processor
IncapablesEnrichmentProcessor
LEDeletionEnrichmentProcessor
LegalEntityChildEnrichmentProcessor
LEParentEnrichmentProcessor
OrganisationUnitEnrichmentProcessor
schedular
BatchSchedular
NotificationSchedular
tasklet
LEDeletionApiTasklet
OrganisationSearchApiTasklet
OrganisationUnitSearchApiTasklet
configreader
dao
dbfields
queries
AccountingDAO
ApiTransactionLogDAO
ErrorLogDAO
FileIngestionDAO
FlowType
IncapablesAccountingDAO
LegalEntityDeletionDAO
MappingDAO
OrgChildAccountingDAO
OrgParentAccountingDAO
OrgUnitAccountingDAO
domain
fileinput
incapables
FileIngestionDomain
LegalEntityChildDomain
LegalEntityDeletionDomain
LegalEntityParentDomain
MappingTableRecord
OrganisationUnitDomain
OrganisationUnitDomainWrapper
TaskletCofaceChildDomainWrapper
TaskletCofaceOpsDomainWrapper
TaskletCofaceParentDomainWrapper
TaskletLeDeletionDomainWrapper
ecs
configuration
BucketConfiguration
services
BucketService
FileProviderService
FileUploadService
KafkaEcsFileProducerService
enums
ErrorType
FileIngestionStatus
RecordType
kafka
constants
MessageConstants
consumer
config
ConsumerConfigs
KafkaConfigKeys
KafkaConsumerConfigFactory
KafkaProducerConfigFactory
SSLConfigs
service
common
DigitalAddressService
IndustryClassificationConsumerService
PostalAddressService
incapable
IncapableIndividualConsumerService
IncapableIndividualNameConsumerService
IncapableMarkingConsumerService
IncapableOccupationConsumerService
involvedparty
InvolvedPartyExternalIdentifierConsumerService
InvolvedPartyInternalIdentifierConsumerService
legalentity
AssessmentConsumerService
OrganisationConsumerService
OrganisationNameConsumerService
organisationunit
OrganisationHierarchyConsumerService
OrganisationUnitConsumerService
OrganisationUnitNameConsumerService
domain
DigitalAddress
ExternalIdentifier
IncapableDomainWrapper
Individual
IndividualName
LegalEntityChildDomain
LegalEntityParentDomain
Marking
Occupation
OrganisationUnitDomainWrapper
PostalAddress
exception
EmptyPropsException
MessagePublishingException
NullKeyOrValueException
PropsBuilderException
ValidatorException
util
ComparisonResult
EventTransactionType
NotificationProcessingUtil
NotificationPropertyNames
NotificationStatus
NotificationType
PropsBuilder
validator
incapable
AdministratorNameValidator
AdministratorPostalAddressValidator
AdministratorValidator
IndividualNameValidator
IndividualValidator
MarkingValidator
OccupationValidator
PostalAddressValidator
ops
DigitalAddressValidator
KBOExternalIdentifierValidator
OrganisationUnitNameValidator
OrganisationUnitValidator
PostalAddressValidator
org
AbstractValidator
buildComparisonResult
Validator
ValidatorFactory
oil
v5_4_0
service
CreateIndividual
CreateIPtoIP
CreateJsonFile
CreateOrganisation
CreateOrganisationHierarchy
CreateOrganisationUnit
OilCreateIDVFileLogicService
OilCreateLEFileLogicService
OilCreateOUFileLogicService
OilFileName
retention
config
DataRetentionConfig
dao
DataRetentionDAO
domain
RetentionResult
scheduler
DataRetentionScheduler
service
DataRetentionService
README.md
service
FileIngestionService
util
CsvTotalRecordsParser
FileUtility
DataDistributionApplication
main
resources
avro
COFACE_LE_DEL.csv
CofaceIncapables.csv
CofaceLEChild.csv
cofaceLEParent.csv
cofaceOpsData.csv
ddsql.sql
test
target
pom.xml
this is the issue im getting
12/03/2026 14:02:01,796 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || o.s.batch.core.job.SimpleStepHandler || handleStep || Executing step: [legalEntityParentEnrichStep]
12/03/2026 14:02:01,796 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Executing: id=283399
12/03/2026 14:02:01,799 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.update]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:01,800 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1605324614 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:01,800 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1605324614 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:01,800 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,801 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:01,803 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:01,804 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:01,806 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:01,806 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1605324614 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:01,808 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1605324614 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:01,815 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.b.p.LEParentEnrichmentProcessor || beforeStep ||  Preloading mapping table into memory
12/03/2026 14:02:01,818 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:01,818 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [select DD_UUID, ORG_NUMBER FROM DD_MAP_TBL  WHERE TYP_OF_ENTY=?]
12/03/2026 14:02:01,818 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.datasource.DataSourceUtils || doGetConnection || Fetching JDBC Connection from DataSource
12/03/2026 14:02:01,826 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.b.p.LEParentEnrichmentProcessor || beforeStep || Loaded 9 UUIDs from mapping table
12/03/2026 14:02:01,828 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.StepScope || get || Creating object in scope=step, name=scopedTarget.legalEntityParentReader
12/03/2026 14:02:01,836 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.batch.config.BatchJobConfig || getFileForLocalRun || DD_Data_Distributor Read File from RESOURCES, filename:cofaceLEParent.csv
12/03/2026 14:02:01,836 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.batch.config.BatchJobConfig || getFileForLocalRun || DD_Data_Distributor File Downloaded Successfully...!
12/03/2026 14:02:01,837 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.service.FileIngestionService || updateTotalNumberOfRecords || File Ingestion Service: footer-based totalRecords detected: 3 (fileId: FILE_1773320520035_57c763bb)
12/03/2026 14:02:01,839 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,839 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE DD_FILE_INGESTION_TBL SET TOTAL_RECORDS = ?,FILE_SIZE = ? WHERE FILE_ID = ?]
12/03/2026 14:02:01,839 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.datasource.DataSourceUtils || doGetConnection || Fetching JDBC Connection from DataSource
12/03/2026 14:02:01,842 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.ing.datadist.dao.FileIngestionDAO || updateTotalNumberOfRecords || File Ingestion totalRecords updated for FILE_ID: FILE_1773320520035_57c763bb, totalRecords: 3, fileSize:718
12/03/2026 14:02:01,853 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.StepScope || registerDestructionCallback || Registered destruction callback in scope=step, name=scopedTarget.legalEntityParentReader
12/03/2026 14:02:01,856 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:01,857 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@595606654 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:01,857 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@595606654 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:01,859 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,860 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:01,862 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:01,863 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@595606654 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:01,865 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@595606654 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:01,883 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || start || Starting repeat context.
12/03/2026 14:02:01,885 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=1
12/03/2026 14:02:01,886 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.c.StepContextRepeatCallback || doInIteration || Preparing chunk execution for StepContext: org.springframework.batch.core.scope.context.StepContext@7afb5297
12/03/2026 14:02:01,886 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.c.StepContextRepeatCallback || doInIteration || Chunk execution starting: queue size=0
12/03/2026 14:02:01,888 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [null]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:01,889 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@664485739 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:01,889 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@664485739 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:01,891 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || start || Starting repeat context.
12/03/2026 14:02:01,891 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=1
12/03/2026 14:02:01,897 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=2
12/03/2026 14:02:01,899 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 3 in resource=[class path resource [cofaceLEParent.csv]], input=[3] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 14:02:01,899 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || read || Skipping failed input
org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 3 in resource=[class path resource [cofaceLEParent.csv]], input=[3]
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:198)
	at org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader.read(AbstractItemCountingItemStreamItemReader.java:93)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:360)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:196)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)
	at org.springframework.aop.support.DelegatingIntroductionInterceptor.doProceed(DelegatingIntroductionInterceptor.java:137)
	at org.springframework.aop.support.DelegatingIntroductionInterceptor.invoke(DelegatingIntroductionInterceptor.java:124)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:184)
	at org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:728)
	at org.springframework.batch.item.file.FlatFileItemReader$$SpringCGLIB$$0.read(<generated>)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.doRead(SimpleChunkProvider.java:108)
	at org.springframework.batch.core.step.item.FaultTolerantChunkProvider.read(FaultTolerantChunkProvider.java:86)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.lambda$provide$0(SimpleChunkProvider.java:132)
	at org.springframework.batch.repeat.support.RepeatTemplate.getNextResult(RepeatTemplate.java:369)
	at org.springframework.batch.repeat.support.RepeatTemplate.executeInternal(RepeatTemplate.java:206)
	at org.springframework.batch.repeat.support.RepeatTemplate.iterate(RepeatTemplate.java:140)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.provide(SimpleChunkProvider.java:127)
	at org.springframework.batch.core.step.item.ChunkOrientedTasklet.execute(ChunkOrientedTasklet.java:69)
	at org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:383)
	at org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:307)
	at org.springframework.transaction.support.TransactionTemplate.execute(TransactionTemplate.java:140)
	at org.springframework.batch.core.step.tasklet.TaskletStep$2.doInChunkContext(TaskletStep.java:250)
	at org.springframework.batch.core.scope.context.StepContextRepeatCallback.doInIteration(StepContextRepeatCallback.java:82)
	at org.springframework.batch.repeat.support.RepeatTemplate.getNextResult(RepeatTemplate.java:369)
	at org.springframework.batch.repeat.support.RepeatTemplate.executeInternal(RepeatTemplate.java:206)
	at org.springframework.batch.repeat.support.RepeatTemplate.iterate(RepeatTemplate.java:140)
	at org.springframework.batch.core.step.tasklet.TaskletStep.doExecute(TaskletStep.java:235)
	at org.springframework.batch.core.step.AbstractStep.execute(AbstractStep.java:230)
	at org.springframework.batch.core.job.SimpleStepHandler.handleStep(SimpleStepHandler.java:153)
	at org.springframework.batch.core.job.AbstractJob.handleStep(AbstractJob.java:408)
	at org.springframework.batch.core.job.SimpleJob.doExecute(SimpleJob.java:127)
	at org.springframework.batch.core.job.AbstractJob.execute(AbstractJob.java:307)
	at org.springframework.batch.core.launch.support.TaskExecutorJobLauncher$1.run(TaskExecutorJobLauncher.java:155)
	at org.springframework.core.task.SyncTaskExecutor.execute(SyncTaskExecutor.java:48)
	at org.springframework.batch.core.launch.support.TaskExecutorJobLauncher.run(TaskExecutorJobLauncher.java:146)
	at com.ing.datadist.batch.schedular.BatchSchedular.runLE(BatchSchedular.java:281)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.runInternal(ScheduledMethodRunnable.java:130)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.lambda$run$2(ScheduledMethodRunnable.java:124)
	at io.micrometer.observation.Observation.observe(Observation.java:498)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.run(ScheduledMethodRunnable.java:124)
	at org.springframework.scheduling.config.Task$OutcomeTrackingRunnable.run(Task.java:87)
	at org.springframework.scheduling.support.DelegatingErrorHandlingRunnable.run(DelegatingErrorHandlingRunnable.java:54)
	at org.springframework.scheduling.concurrent.ReschedulingRunnable.run(ReschedulingRunnable.java:96)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:304)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: org.springframework.batch.item.file.transform.IncorrectTokenCountException: Incorrect number of tokens found in record: expected 21 actual 1
	at org.springframework.batch.item.file.transform.AbstractLineTokenizer.tokenize(AbstractLineTokenizer.java:134)
	at org.springframework.batch.item.file.mapping.DefaultLineMapper.mapLine(DefaultLineMapper.java:42)
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:194)
	... 52 common frames omitted
12/03/2026 14:02:01,903 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 14:02:01,912 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:01,913 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.JobScope || get || Creating object in scope=job, name=scopedTarget.taskletCofaceParentDomainWrapper
12/03/2026 14:02:01,921 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProcessor || write || Attempting to write: [items=[LegalEntityParentDomain(leExternalIdentifier=0607935622, orgStatus=AC, orgName=WOLUWE SHOPPING OPTIC, orgOtherName=WS OPTIC, orgOtherNameType=abbrev, adrUnstructuredAddress=Bd de la Woluwe 70 B.103, adrPostalAddressStreetName=Bd de la Woluwe, adrPostalAddressHouseNum=70, adrPostalAddressHouseNumAdd=103, adrPostalAddressPostalCode=1200, adrPostalAddressCityName=BRUXELLES, adrPostalAddressCountryCode=BE, orgLegalForm=015, orgBusinessClosedownDate=, lELegalStatus=000, asmtIdVerifyApprovalDate=24/03/2015, lEPreferredLanguage=1, adrDigitalAddrFullTel=02/771.60.65, nssoExternalIdentifier=140372094, asmtVatStatus=0, nssoExternalIdentifierStatus=1, orgUUID=2ef682f7-cd9b-4433-96f0-32ad515d9983, batchId=null, fileName=null, fileId=FILE_1773320520035_57c763bb)], skips=[]]
12/03/2026 14:02:01,922 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:01,922 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.i.database.JdbcBatchItemWriter || write || Executing batch with 1 items.
12/03/2026 14:02:01,932 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || batchUpdate || Executing SQL batch update [INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, FILE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
12/03/2026 14:02:01,933 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, FILE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
12/03/2026 14:02:01,934 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.support.JdbcUtils || supportsBatchUpdates || JDBC driver supports batch updates
12/03/2026 14:02:01,940 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.item.ChunkOrientedTasklet || execute || Inputs not busy, ended: true
12/03/2026 14:02:01,944 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=1, written=1, filtered=0, readSkips=1, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 14:02:01,945 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 14:02:01,945 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,946 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:01,948 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Saving step execution before commit: StepExecution: id=283399, version=1, name=legalEntityParentEnrichStep, status=STARTED, exitStatus=EXECUTING, readCount=1, filterCount=0, writeCount=1, readSkipCount=1, writeSkipCount=0, processSkipCount=0, commitCount=1, rollbackCount=0, exitDescription=
12/03/2026 14:02:01,949 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 14:02:01,949 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,949 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:01,951 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:01,951 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:01,979 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:01,980 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@664485739 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:01,983 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@664485739 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:01,983 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 14:02:01,984 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Step execution success: id=283399
12/03/2026 14:02:01,987 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || o.s.batch.core.step.AbstractStep || execute || Step: [legalEntityParentEnrichStep] executed in 190ms
12/03/2026 14:02:01,988 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:01,988 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@236220241 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:01,989 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@236220241 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:01,990 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,990 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:01,994 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:01,994 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@236220241 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:01,996 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@236220241 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:01,996 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.update]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:01,997 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@854695502 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:01,997 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@854695502 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:01,997 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:01,997 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:02,000 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,000 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:02,003 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,003 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@854695502 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,005 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@854695502 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,006 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Step execution complete: StepExecution: id=283399, version=3, name=legalEntityParentEnrichStep, status=COMPLETED, exitStatus=COMPLETED, readCount=1, filterCount=0, writeCount=1, readSkipCount=1, writeSkipCount=0, processSkipCount=0, commitCount=1, rollbackCount=0
12/03/2026 14:02:02,006 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,006 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1450610177 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,006 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1450610177 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,008 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,008 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_JOB_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE JOB_EXECUTION_ID = ?
]
12/03/2026 14:02:02,011 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,011 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1450610177 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,013 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1450610177 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,013 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.getLastStepExecution]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,014 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1807292125 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,014 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1807292125 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,014 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,014 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT SE.STEP_EXECUTION_ID, SE.STEP_NAME, SE.START_TIME, SE.END_TIME, SE.STATUS, SE.COMMIT_COUNT, SE.READ_COUNT, SE.FILTER_COUNT, SE.WRITE_COUNT, SE.EXIT_CODE, SE.EXIT_MESSAGE, SE.READ_SKIP_COUNT, SE.WRITE_SKIP_COUNT, SE.PROCESS_SKIP_COUNT, SE.ROLLBACK_COUNT, SE.LAST_UPDATED, SE.VERSION, SE.CREATE_TIME, JE.JOB_EXECUTION_ID, JE.START_TIME, JE.END_TIME, JE.STATUS, JE.EXIT_CODE, JE.EXIT_MESSAGE, JE.CREATE_TIME, JE.LAST_UPDATED, JE.VERSION
FROM BATCH_JOB_EXECUTION JE
	JOIN BATCH_STEP_EXECUTION SE ON SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID
WHERE JE.JOB_INSTANCE_ID = ? AND SE.STEP_NAME = ?
]
12/03/2026 14:02:02,032 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,032 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1807292125 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,033 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1807292125 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,033 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.getStepExecutionCount]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,033 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@715137235 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,034 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@715137235 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,034 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,034 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT COUNT(*)
FROM BATCH_JOB_EXECUTION JE
	JOIN BATCH_STEP_EXECUTION SE ON SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID
WHERE JE.JOB_INSTANCE_ID = ? AND SE.STEP_NAME = ?
]
12/03/2026 14:02:02,052 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,053 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@715137235 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,053 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@715137235 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,053 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.add]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,054 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1047092391 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,054 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1047092391 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,057 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,057 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO BATCH_STEP_EXECUTION(STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, CREATE_TIME)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
]
12/03/2026 14:02:02,060 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,061 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO BATCH_STEP_EXECUTION_CONTEXT (SHORT_CONTEXT, SERIALIZED_CONTEXT, STEP_EXECUTION_ID)
	VALUES(?, ?, ?)
]
12/03/2026 14:02:02,063 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,064 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1047092391 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,066 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1047092391 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,066 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || o.s.batch.core.job.SimpleStepHandler || handleStep || Executing step: [legalEntityChildEnrichStep]
12/03/2026 14:02:02,066 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Executing: id=283400
12/03/2026 14:02:02,067 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.update]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,067 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@310145616 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,067 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@310145616 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,067 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,067 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:02,071 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,071 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:02,073 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,074 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@310145616 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,078 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@310145616 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,079 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.StepScope || get || Creating object in scope=step, name=scopedTarget.legalEntityChildReader
12/03/2026 14:02:02,080 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.batch.config.BatchJobConfig || getFileForLocalRun || DD_Data_Distributor Read File from RESOURCES, filename:CofaceLEChild.csv
12/03/2026 14:02:02,080 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.batch.config.BatchJobConfig || getFileForLocalRun || DD_Data_Distributor File Downloaded Successfully...!
12/03/2026 14:02:02,081 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.i.d.service.FileIngestionService || updateTotalNumberOfRecords || File Ingestion Service: footer-based totalRecords detected: 5 (fileId: FILE_1773320521329_09f07840)
12/03/2026 14:02:02,081 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,082 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE DD_FILE_INGESTION_TBL SET TOTAL_RECORDS = ?,FILE_SIZE = ? WHERE FILE_ID = ?]
12/03/2026 14:02:02,083 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.datasource.DataSourceUtils || doGetConnection || Fetching JDBC Connection from DataSource
12/03/2026 14:02:02,085 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || c.ing.datadist.dao.FileIngestionDAO || updateTotalNumberOfRecords || File Ingestion totalRecords updated for FILE_ID: FILE_1773320521329_09f07840, totalRecords: 5, fileSize:370
12/03/2026 14:02:02,087 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.StepScope || registerDestructionCallback || Registered destruction callback in scope=step, name=scopedTarget.legalEntityChildReader
12/03/2026 14:02:02,088 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,089 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1203217414 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,089 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1203217414 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,089 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,089 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:02,092 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,092 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1203217414 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,094 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1203217414 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,094 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || start || Starting repeat context.
12/03/2026 14:02:02,094 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=1
12/03/2026 14:02:02,094 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.c.StepContextRepeatCallback || doInIteration || Preparing chunk execution for StepContext: org.springframework.batch.core.scope.context.StepContext@4a7ae04e
12/03/2026 14:02:02,095 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.c.StepContextRepeatCallback || doInIteration || Chunk execution starting: queue size=0
12/03/2026 14:02:02,095 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [null]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,095 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@877316852 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,095 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@877316852 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,095 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || start || Starting repeat context.
12/03/2026 14:02:02,096 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=1
12/03/2026 14:02:02,097 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=2
12/03/2026 14:02:02,098 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=3
12/03/2026 14:02:02,099 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=4
12/03/2026 14:02:02,099 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=5
12/03/2026 14:02:02,100 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=6
12/03/2026 14:02:02,100 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 7 in resource=[class path resource [CofaceLEChild.csv]], input=[5] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 14:02:02,101 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || read || Skipping failed input
org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 7 in resource=[class path resource [CofaceLEChild.csv]], input=[5]
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:198)
	at org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader.read(AbstractItemCountingItemStreamItemReader.java:93)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:360)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:196)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)
	at org.springframework.aop.support.DelegatingIntroductionInterceptor.doProceed(DelegatingIntroductionInterceptor.java:137)
	at org.springframework.aop.support.DelegatingIntroductionInterceptor.invoke(DelegatingIntroductionInterceptor.java:124)
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:184)
	at org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:728)
	at org.springframework.batch.item.file.FlatFileItemReader$$SpringCGLIB$$0.read(<generated>)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.doRead(SimpleChunkProvider.java:108)
	at org.springframework.batch.core.step.item.FaultTolerantChunkProvider.read(FaultTolerantChunkProvider.java:86)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.lambda$provide$0(SimpleChunkProvider.java:132)
	at org.springframework.batch.repeat.support.RepeatTemplate.getNextResult(RepeatTemplate.java:369)
	at org.springframework.batch.repeat.support.RepeatTemplate.executeInternal(RepeatTemplate.java:206)
	at org.springframework.batch.repeat.support.RepeatTemplate.iterate(RepeatTemplate.java:140)
	at org.springframework.batch.core.step.item.SimpleChunkProvider.provide(SimpleChunkProvider.java:127)
	at org.springframework.batch.core.step.item.ChunkOrientedTasklet.execute(ChunkOrientedTasklet.java:69)
	at org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:383)
	at org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:307)
	at org.springframework.transaction.support.TransactionTemplate.execute(TransactionTemplate.java:140)
	at org.springframework.batch.core.step.tasklet.TaskletStep$2.doInChunkContext(TaskletStep.java:250)
	at org.springframework.batch.core.scope.context.StepContextRepeatCallback.doInIteration(StepContextRepeatCallback.java:82)
	at org.springframework.batch.repeat.support.RepeatTemplate.getNextResult(RepeatTemplate.java:369)
	at org.springframework.batch.repeat.support.RepeatTemplate.executeInternal(RepeatTemplate.java:206)
	at org.springframework.batch.repeat.support.RepeatTemplate.iterate(RepeatTemplate.java:140)
	at org.springframework.batch.core.step.tasklet.TaskletStep.doExecute(TaskletStep.java:235)
	at org.springframework.batch.core.step.AbstractStep.execute(AbstractStep.java:230)
	at org.springframework.batch.core.job.SimpleStepHandler.handleStep(SimpleStepHandler.java:153)
	at org.springframework.batch.core.job.AbstractJob.handleStep(AbstractJob.java:408)
	at org.springframework.batch.core.job.SimpleJob.doExecute(SimpleJob.java:127)
	at org.springframework.batch.core.job.AbstractJob.execute(AbstractJob.java:307)
	at org.springframework.batch.core.launch.support.TaskExecutorJobLauncher$1.run(TaskExecutorJobLauncher.java:155)
	at org.springframework.core.task.SyncTaskExecutor.execute(SyncTaskExecutor.java:48)
	at org.springframework.batch.core.launch.support.TaskExecutorJobLauncher.run(TaskExecutorJobLauncher.java:146)
	at com.ing.datadist.batch.schedular.BatchSchedular.runLE(BatchSchedular.java:281)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.runInternal(ScheduledMethodRunnable.java:130)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.lambda$run$2(ScheduledMethodRunnable.java:124)
	at io.micrometer.observation.Observation.observe(Observation.java:498)
	at org.springframework.scheduling.support.ScheduledMethodRunnable.run(ScheduledMethodRunnable.java:124)
	at org.springframework.scheduling.config.Task$OutcomeTrackingRunnable.run(Task.java:87)
	at org.springframework.scheduling.support.DelegatingErrorHandlingRunnable.run(DelegatingErrorHandlingRunnable.java:54)
	at org.springframework.scheduling.concurrent.ReschedulingRunnable.run(ReschedulingRunnable.java:96)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:304)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: org.springframework.batch.item.file.transform.IncorrectTokenCountException: Incorrect number of tokens found in record: expected 5 actual 1
	at org.springframework.batch.item.file.transform.AbstractLineTokenizer.tokenize(AbstractLineTokenizer.java:134)
	at org.springframework.batch.item.file.mapping.DefaultLineMapper.mapLine(DefaultLineMapper.java:42)
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:194)
	... 52 common frames omitted
12/03/2026 14:02:02,101 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 14:02:02,102 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,102 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,103 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 14:02:02,106 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.JobScope || get || Creating object in scope=job, name=scopedTarget.taskletCofaceChildDomainWrapper
12/03/2026 14:02:02,109 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,109 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,110 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 14:02:02,113 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,113 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,113 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 14:02:02,116 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,117 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,117 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 14:02:02,121 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,122 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,122 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 14:02:02,128 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProcessor || write || Attempting to write: [items=[LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=84114, industryClassRank=1, industryClassEffDate=01-03-2002, industryClassEndDate=12-12-2028, batchId=null, fileName=null, fileId=FILE_1773320521329_09f07840), LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=15200, industryClassRank=1, industryClassEffDate=23-05-1947, industryClassEndDate=11-11=2031, batchId=null, fileName=null, fileId=FILE_1773320521329_09f07840), LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=47721, industryClassRank=1, industryClassEffDate=01-01-2008, industryClassEndDate=07-07-2028, batchId=null, fileName=null, fileId=FILE_1773320521329_09f07840), LegalEntityChildDomain(orgUUID=f29638fa-90ab-43d9-a191-766a253f9198, leExternalIdentifier=0607927605, industryClassCode=47529, industryClassRank=1, industryClassEffDate=07/05/2015, industryClassEndDate=02/04/2027, batchId=null, fileName=null, fileId=FILE_1773320521329_09f07840), LegalEntityChildDomain(orgUUID=2ef682f7-cd9b-4433-96f0-32ad515d9983, leExternalIdentifier=0607935622, industryClassCode=47742, industryClassRank=1, industryClassEffDate=24/03/2015, industryClassEndDate=13/01/2027, batchId=null, fileName=null, fileId=FILE_1773320521329_09f07840)], skips=[]]
12/03/2026 14:02:02,129 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 14:02:02,129 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.i.database.JdbcBatchItemWriter || write || Executing batch with 5 items.
12/03/2026 14:02:02,130 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || batchUpdate || Executing SQL batch update [INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, FILE_ID) VALUES (?, ?, ?, ?, ?, ?)]
12/03/2026 14:02:02,131 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, FILE_ID) VALUES (?, ?, ?, ?, ?, ?)]
12/03/2026 14:02:02,131 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.support.JdbcUtils || supportsBatchUpdates || JDBC driver supports batch updates
12/03/2026 14:02:02,135 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.c.s.item.ChunkOrientedTasklet || execute || Inputs not busy, ended: true
12/03/2026 14:02:02,135 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=5, written=5, filtered=0, readSkips=1, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 14:02:02,135 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 14:02:02,136 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,136 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:02,139 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Saving step execution before commit: StepExecution: id=283400, version=1, name=legalEntityChildEnrichStep, status=STARTED, exitStatus=EXECUTING, readCount=5, filterCount=0, writeCount=5, readSkipCount=1, writeSkipCount=0, processSkipCount=0, commitCount=1, rollbackCount=0, exitDescription=
12/03/2026 14:02:02,139 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 14:02:02,140 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,140 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:02,142 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,143 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:02,180 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,180 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@877316852 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,183 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@877316852 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,184 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 14:02:02,184 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Step execution success: id=283400
12/03/2026 14:02:02,185 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || INFO  || o.s.batch.core.step.AbstractStep || execute || Step: [legalEntityChildEnrichStep] executed in 118ms
12/03/2026 14:02:02,188 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,188 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1913048911 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,188 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1913048911 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,189 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,189 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 14:02:02,192 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,193 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1913048911 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,195 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1913048911 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,195 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.update]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,195 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1864357607 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,195 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1864357607 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,196 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,196 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 14:02:02,199 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,200 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 14:02:02,203 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,203 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1864357607 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,205 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1864357607 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,206 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.batch.core.step.AbstractStep || execute || Step execution complete: StepExecution: id=283400, version=3, name=legalEntityChildEnrichStep, status=COMPLETED, exitStatus=COMPLETED, readCount=5, filterCount=0, writeCount=5, readSkipCount=1, writeSkipCount=0, processSkipCount=0, commitCount=1, rollbackCount=0
12/03/2026 14:02:02,206 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.updateExecutionContext]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,207 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@736359328 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,207 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@736359328 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,208 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,208 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_JOB_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE JOB_EXECUTION_ID = ?
]
12/03/2026 14:02:02,211 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,211 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@736359328 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,213 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@736359328 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,213 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.getLastStepExecution]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,214 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@2071738995 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,214 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@2071738995 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,214 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,214 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT SE.STEP_EXECUTION_ID, SE.STEP_NAME, SE.START_TIME, SE.END_TIME, SE.STATUS, SE.COMMIT_COUNT, SE.READ_COUNT, SE.FILTER_COUNT, SE.WRITE_COUNT, SE.EXIT_CODE, SE.EXIT_MESSAGE, SE.READ_SKIP_COUNT, SE.WRITE_SKIP_COUNT, SE.PROCESS_SKIP_COUNT, SE.ROLLBACK_COUNT, SE.LAST_UPDATED, SE.VERSION, SE.CREATE_TIME, JE.JOB_EXECUTION_ID, JE.START_TIME, JE.END_TIME, JE.STATUS, JE.EXIT_CODE, JE.EXIT_MESSAGE, JE.CREATE_TIME, JE.LAST_UPDATED, JE.VERSION
FROM BATCH_JOB_EXECUTION JE
	JOIN BATCH_STEP_EXECUTION SE ON SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID
WHERE JE.JOB_INSTANCE_ID = ? AND SE.STEP_NAME = ?
]
12/03/2026 14:02:02,232 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,233 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@2071738995 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,233 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@2071738995 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,233 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.getStepExecutionCount]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,234 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@1920749885 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,234 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1920749885 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,235 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 14:02:02,235 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT COUNT(*)
FROM BATCH_JOB_EXECUTION JE
	JOIN BATCH_STEP_EXECUTION SE ON SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID
WHERE JE.JOB_INSTANCE_ID = ? AND SE.STEP_NAME = ?
]
12/03/2026 14:02:02,252 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
12/03/2026 14:02:02,253 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCommit || Committing JDBC transaction on Connection [HikariProxyConnection@1920749885 wrapping oracle.jdbc.driver.T4CConnection@611b35d6]
12/03/2026 14:02:02,253 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doCleanupAfterCompletion || Releasing JDBC Connection [HikariProxyConnection@1920749885 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] after transaction
12/03/2026 14:02:02,254 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || getTransaction || Creating new transaction with name [org.springframework.batch.core.repository.support.SimpleJobRepository.add]: PROPAGATION_REQUIRED,ISOLATION_DEFAULT
12/03/2026 14:02:02,254 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Acquired Connection [HikariProxyConnection@2044307221 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] for JDBC transaction
12/03/2026 14:02:02,255 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@2044307221 wrapping oracle.jdbc.driver.T4CConnection@611b35d6] to manual commit
12/03/2026 14:02:02,261 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,262 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO BATCH_STEP_EXECUTION(STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, CREATE_TIME)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
]
12/03/2026 14:02:02,265 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 14:02:02,265 || datadistributorapiservice-667b4454cd-s7q6t || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO BATCH_STEP_EXECUTION_CONTEXT (SHORT_CONTEXT, SERIALIZED_CONTEXT, STEP_EXECUTION_ID)
	VALUES(?, ?, ?)
]
give files that u willl need to resolve this error 
