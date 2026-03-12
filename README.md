12/03/2026 13:20:01,766 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=4
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 5 in resource=[class path resource [CofaceIncapables.csv]], input=[3] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || doHandle || Handling exception: org.springframework.batch.core.step.skip.NonSkippableReadException, caused by: org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || executeInternal || Handling fatal exception explicitly (rethrowing first of 1): org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=3, written=0, filtered=0, readSkips=0, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Rollback for RuntimeException: org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.t.support.TransactionTemplate || rollbackOnException || Initiating transaction rollback on application exception
org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
	at org.springframework.batch.core.step.item.FaultTolerantChunkProvider.read(FaultTolerantChunkProvider.java:104)
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
	at com.ing.datadist.batch.schedular.BatchSchedular.runIncapables(BatchSchedular.java:463)
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
Caused by: org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 5 in resource=[class path resource [CofaceIncapables.csv]], input=[3]
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
	... 39 common frames omitted
Caused by: org.springframework.batch.item.file.transform.IncorrectTokenCountException: Incorrect number of tokens found in record: expected 26 actual 1
	at org.springframework.batch.item.file.transform.AbstractLineTokenizer.tokenize(AbstractLineTokenizer.java:134)
	at org.springframework.batch.item.file.mapping.DefaultLineMapper.mapLine(DefaultLineMapper.java:42)
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:194)
	... 52 common frames omitted
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processRollback || Initiating transaction rollback
12/03/2026 13:20:01,766 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=4
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 5 in resource=[class path resource [CofaceIncapables.csv]], input=[3] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || doHandle || Handling exception: org.springframework.batch.core.step.skip.NonSkippableReadException, caused by: org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || executeInternal || Handling fatal exception explicitly (rethrowing first of 1): org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,767 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=3, written=0, filtered=0, readSkips=0, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Rollback for RuntimeException: org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.t.support.TransactionTemplate || rollbackOnException || Initiating transaction rollback on application exception
org.springframework.batch.core.step.skip.NonSkippableReadException: Non-skippable exception during read
	at org.springframework.batch.core.step.item.FaultTolerantChunkProvider.read(FaultTolerantChunkProvider.java:104)
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
	at com.ing.datadist.batch.schedular.BatchSchedular.runIncapables(BatchSchedular.java:463)
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
Caused by: org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 5 in resource=[class path resource [CofaceIncapables.csv]], input=[3]
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
	... 39 common frames omitted
Caused by: org.springframework.batch.item.file.transform.IncorrectTokenCountException: Incorrect number of tokens found in record: expected 26 actual 1
	at org.springframework.batch.item.file.transform.AbstractLineTokenizer.tokenize(AbstractLineTokenizer.java:134)
	at org.springframework.batch.item.file.mapping.DefaultLineMapper.mapLine(DefaultLineMapper.java:42)
	at org.springframework.batch.item.file.FlatFileItemReader.doRead(FlatFileItemReader.java:194)
	... 52 common frames omitted
12/03/2026 13:20:01,768 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processRollback || Initiating transaction rollback
12/03/2026 13:20:01,904 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || doBegin || Switching JDBC Connection [HikariProxyConnection@1140451054 wrapping oracle.jdbc.driver.T4CConnection@40c06358] to manual commit
12/03/2026 13:20:01,904 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || start || Starting repeat context.
12/03/2026 13:20:01,904 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=1
12/03/2026 13:20:01,905 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=2
12/03/2026 13:20:01,905 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=3
12/03/2026 13:20:01,905 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || getNextResult || Repeat operation about to start at count=4
12/03/2026 13:20:01,906 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 5 in resource=[class path resource [cofaceLEParent.csv]], input=[3] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 13:20:01,906 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || read || Skipping failed input
org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 5 in resource=[class path resource [cofaceLEParent.csv]], input=[3]
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
	at com.ing.datadist.batch.schedular.BatchSchedular.runLE(BatchSchedular.java:278)
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
12/03/2026 13:20:01,906 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 13:20:01,906 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,906 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.JobScope || get || Creating object in scope=job, name=scopedTarget.taskletCofaceParentDomainWrapper
12/03/2026 13:20:01,907 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,907 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,912 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProcessor || write || Attempting to write: [items=[LegalEntityParentDomain(leExternalIdentifier=1017306504, orgStatus=AC, orgName=VOITHUUR, orgOtherName=, orgOtherNameType=, adrUnstructuredAddress=Moenkouterstraat 5, adrPostalAddressStreetName=Moenkouterstraat, adrPostalAddressHouseNum=5, adrPostalAddressHouseNumAdd=, adrPostalAddressPostalCode=8552, adrPostalAddressCityName=MOEN, adrPostalAddressCountryCode=, orgLegalForm=014, orgBusinessClosedownDate=, lELegalStatus=000, asmtIdVerifyApprovalDate=, lEPreferredLanguage=3, adrDigitalAddrFullTel="+3265654566", nssoExternalIdentifier=180774437, asmtVatStatus=0, nssoExternalIdentifierStatus=0, orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, batchId=null, fileName=null, fileId=FILE_1773318001793_a953ca28), LegalEntityParentDomain(leExternalIdentifier=1001841041, orgStatus=AC, orgName=JONCKHEERE.WOOD, orgOtherName=, orgOtherNameType=, adrUnstructuredAddress=Z. 5 Mollem 5, adrPostalAddressStreetName=Z. 5 Mollem, adrPostalAddressHouseNum=5, adrPostalAddressHouseNumAdd=, adrPostalAddressPostalCode=1730, adrPostalAddressCityName=MOLLEM, adrPostalAddressCountryCode=, orgLegalForm=014, orgBusinessClosedownDate=, lELegalStatus=000, asmtIdVerifyApprovalDate=, lEPreferredLanguage=3, adrDigitalAddrFullTel="+3224540330", nssoExternalIdentifier=, asmtVatStatus=0, nssoExternalIdentifierStatus=0, orgUUID=0f8e8133-7e6d-470d-8d67-caa9f80e75cc, batchId=null, fileName=null, fileId=FILE_1773318001793_a953ca28), LegalEntityParentDomain(leExternalIdentifier=1001000001, orgStatus=AC, orgName=ABC Industries NV, orgOtherName=ABC IND, orgOtherNameType=abbrev, adrUnstructuredAddress=Koningsstraat 100, adrPostalAddressStreetName=Koningsstraat, adrPostalAddressHouseNum=100, adrPostalAddressHouseNumAdd=, adrPostalAddressPostalCode=1000, adrPostalAddressCityName=BRUSSEL, adrPostalAddressCountryCode=BE, orgLegalForm=014, orgBusinessClosedownDate=, lELegalStatus=000, asmtIdVerifyApprovalDate=, lEPreferredLanguage=1, adrDigitalAddrFullTel=+3222223344, nssoExternalIdentifier=100000001, asmtVatStatus=0, nssoExternalIdentifierStatus=0, orgUUID=4704eea5-8fba-4693-80fd-ba5d60beb6ca, batchId=null, fileName=null, fileId=FILE_1773318001793_a953ca28)], skips=[]]
12/03/2026 13:20:01,912 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,912 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.i.database.JdbcBatchItemWriter || write || Executing batch with 3 items.
12/03/2026 13:20:01,913 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || batchUpdate || Executing SQL batch update [INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, FILE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
12/03/2026 13:20:01,913 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, FILE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]
12/03/2026 13:20:01,914 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.support.JdbcUtils || supportsBatchUpdates || JDBC driver supports batch updates
12/03/2026 13:20:01,917 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.item.ChunkOrientedTasklet || execute || Inputs not busy, ended: true
12/03/2026 13:20:01,917 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=3, written=3, filtered=0, readSkips=1, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 13:20:01,917 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 13:20:01,917 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 13:20:01,917 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
12/03/2026 13:20:01,994 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || doRead || Parsing error at line: 5 in resource=[class path resource [CofaceLEChild.csv]], input=[3] : org.springframework.batch.item.file.FlatFileParseException
12/03/2026 13:20:01,994 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProvider || read || Skipping failed input
org.springframework.batch.item.file.FlatFileParseException: Parsing error at line: 5 in resource=[class path resource [CofaceLEChild.csv]], input=[3]
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
	at com.ing.datadist.batch.schedular.BatchSchedular.runLE(BatchSchedular.java:278)
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
12/03/2026 13:20:01,994 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.repeat.support.RepeatTemplate || isComplete || Repeat is complete according to policy and result value.
12/03/2026 13:20:01,995 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,995 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 13:20:01,995 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 13:20:01,996 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.batch.core.scope.JobScope || get || Creating object in scope=job, name=scopedTarget.taskletCofaceChildDomainWrapper
12/03/2026 13:20:01,997 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,997 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 13:20:01,997 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 13:20:01,998 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:01,998 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 13:20:01,998 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT DD_UUID FROM DD_MAP_TBL WHERE ORG_NUMBER = ?]
12/03/2026 13:20:02,001 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.i.FaultTolerantChunkProcessor || write || Attempting to write: [items=[LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=84114, industryClassRank=1, industryClassEffDate=01-03-2002, industryClassEndDate=, batchId=null, fileName=null, fileId=FILE_1773318001801_c1b33342), LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=15200, industryClassRank=1, industryClassEffDate=23-05-1947, industryClassEndDate=, batchId=null, fileName=null, fileId=FILE_1773318001801_c1b33342), LegalEntityChildDomain(orgUUID=32921cd9-2b80-4111-9616-be2977c9b007, leExternalIdentifier=1017306504, industryClassCode=47721, industryClassRank=1, industryClassEffDate=01-01-2008, industryClassEndDate=, batchId=null, fileName=null, fileId=FILE_1773318001801_c1b33342)], skips=[]]
12/03/2026 13:20:02,001 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.retry.support.RetryTemplate || doExecute || Retry: count=0
12/03/2026 13:20:02,001 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.i.database.JdbcBatchItemWriter || write || Executing batch with 3 items.
12/03/2026 13:20:02,002 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || batchUpdate || Executing SQL batch update [INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, FILE_ID) VALUES (?, ?, ?, ?, ?, ?)]
12/03/2026 13:20:02,002 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, FILE_ID) VALUES (?, ?, ?, ?, ?, ?)]
12/03/2026 13:20:02,002 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.support.JdbcUtils || supportsBatchUpdates || JDBC driver supports batch updates
12/03/2026 13:20:02,003 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.c.s.item.ChunkOrientedTasklet || execute || Inputs not busy, ended: true
12/03/2026 13:20:02,003 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Applying contribution: [StepContribution: read=3, written=3, filtered=0, readSkips=1, writeSkips=0, processSkips=0, exitStatus=EXECUTING]
12/03/2026 13:20:02,004 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 13:20:02,004 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 13:20:02,004 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION_CONTEXT
SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ?
WHERE STEP_EXECUTION_ID = ?
]
12/03/2026 13:20:02,005 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.b.core.step.tasklet.TaskletStep || doInTransaction || Saving step execution before commit: StepExecution: id=283395, version=1, name=legalEntityChildEnrichStep, status=STARTED, exitStatus=EXECUTING, readCount=3, filterCount=0, writeCount=3, readSkipCount=1, writeSkipCount=0, processSkipCount=0, commitCount=1, rollbackCount=0, exitDescription=
12/03/2026 13:20:02,005 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || handleExistingTransaction || Participating in existing transaction
12/03/2026 13:20:02,006 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || update || Executing prepared SQL update
12/03/2026 13:20:02,006 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [UPDATE BATCH_STEP_EXECUTION
SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = VERSION + 1, READ_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?
WHERE STEP_EXECUTION_ID = ? AND VERSION = ?
]
12/03/2026 13:20:02,007 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || query || Executing prepared SQL query
12/03/2026 13:20:02,007 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.jdbc.core.JdbcTemplate || execute || Executing prepared SQL statement [SELECT VERSION
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID=?
]
12/03/2026 13:20:02,008 || datadistributorapiservice-794d8966-jmrz2 || DataDistributorAPIService || DEBUG || o.s.j.support.JdbcTransactionManager || processCommit || Initiating transaction commit
