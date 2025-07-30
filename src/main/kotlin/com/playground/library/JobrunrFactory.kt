package com.playground.library

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import org.axonframework.config.Configuration
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager
import org.axonframework.serialization.Serializer
import org.jobrunr.jobs.mappers.JobMapper
import org.jobrunr.scheduling.JobScheduler
import org.jobrunr.storage.StorageProvider
import org.jobrunr.storage.sql.postgres.PostgresStorageProvider
import javax.sql.DataSource

@Factory
class JobrunrFactory {

    @Singleton
    fun objectMapper(): ObjectMapper = ObjectMapper()

    @Singleton
    fun storageProvider(
        dataSource: DataSource,
        jobMapper: JobMapper
    ): StorageProvider {
        return PostgresStorageProvider(dataSource).apply { setJobMapper(jobMapper) }

    }

//    @Singleton
//    fun jobScheduler(storageProvider: StorageProvider): JobScheduler {
//        return JobScheduler(storageProvider)
//    }

    @Singleton
    fun deadlineManager(
        config: Configuration,
        serializer: Serializer,
        jobScheduler: JobScheduler
    ): JobRunrDeadlineManager {

        return JobRunrDeadlineManager.builder()
            .jobScheduler(jobScheduler)
            .scopeAwareProvider(config.scopeAwareProvider())
            .serializer(serializer)
            .build()
    }
}