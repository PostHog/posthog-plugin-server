import * as Sentry from '@sentry/node'
import { makeWorkerUtils, run, Runner, WorkerUtils, WorkerUtilsOptions } from 'graphile-worker'
import { Pool } from 'pg'

import { EnqueuedJob, JobQueue, OnJobCallback, PluginsServerConfig } from '../../types'
import { status } from '../../utils/status'
import { createPostgresPool } from '../../utils/utils'

export class GraphileQueue implements JobQueue {
    serverConfig: PluginsServerConfig
    started: boolean
    paused: boolean
    onJob: OnJobCallback | null
    runner: Runner | null
    consumerPool: Pool | null
    producerPool: Pool | null
    workerUtilsPromise: Promise<WorkerUtils> | null

    constructor(serverConfig: PluginsServerConfig) {
        this.serverConfig = serverConfig
        this.started = false
        this.paused = false
        this.onJob = null
        this.runner = null
        this.consumerPool = null
        this.producerPool = null
        this.workerUtilsPromise = null
    }

    // producer

    public async migrate(): Promise<void> {
        await (await this.getWorkerUtils()).migrate()
    }

    async connectProducer(): Promise<void> {
        await this.migrate()
    }

    async enqueue(retry: EnqueuedJob): Promise<void> {
        const workerUtils = await this.getWorkerUtils()
        await workerUtils.addJob('pluginJob', retry, {
            runAt: new Date(retry.timestamp),
            maxAttempts: 1,
        })
    }

    private async getWorkerUtils(): Promise<WorkerUtils> {
        if (!this.producerPool) {
            this.producerPool = await this.createPool()
        }
        if (!this.workerUtilsPromise) {
            this.workerUtilsPromise = makeWorkerUtils({
                pgPool: this.producerPool as any,
                schema: this.serverConfig.JOB_QUEUE_GRAPHILE_SCHEMA,
            })
        }
        return await this.workerUtilsPromise
    }

    async disconnectProducer(): Promise<void> {
        const oldWorkerUtils = await this.workerUtilsPromise
        this.workerUtilsPromise = null
        await oldWorkerUtils?.release()
        await this.producerPool?.end()
    }

    // consumer

    async startConsumer(onJob: OnJobCallback): Promise<void> {
        this.started = true
        this.onJob = onJob
        await this.syncState()
    }

    async stopConsumer(): Promise<void> {
        this.started = false
        await this.syncState()
    }

    async pauseConsumer(): Promise<void> {
        this.paused = true
        await this.syncState()
    }

    isConsumerPaused(): boolean {
        return this.paused
    }

    async resumeConsumer(): Promise<void> {
        this.paused = false
        await this.syncState()
    }

    private async syncState(): Promise<void> {
        if (this.started && !this.paused) {
            if (!this.runner) {
                this.consumerPool = await this.createPool()
                this.runner = await run({
                    // graphile's types refer to a local node_modules version of Pool
                    pgPool: (this.consumerPool as Pool) as any,
                    schema: this.serverConfig.JOB_QUEUE_GRAPHILE_SCHEMA,
                    concurrency: 1,
                    // Install signal handlers for graceful shutdown on SIGINT, SIGTERM, etc
                    noHandleSignals: false,
                    pollInterval: 100,
                    // you can set the taskList or taskDirectory but not both
                    taskList: {
                        pluginJob: (payload) => {
                            void this.onJob?.([payload as EnqueuedJob])
                        },
                    },
                })
            }
        } else {
            if (this.runner) {
                const oldRunner = this.runner
                this.runner = null
                await oldRunner?.stop()
                await this.consumerPool?.end()
            }
        }
    }

    private onConnectionError(error: Error) {
        Sentry.captureException(error)
        status.error('🔴', 'Unhandled PostgreSQL error encountered in Graphile Worker!\n', error)

        // TODO: throw a wrench in the gears
    }

    async createPool(): Promise<Pool> {
        return await new Promise(async (resolve, reject) => {
            let resolved = false
            const configOrDatabaseUrl = this.serverConfig.JOB_QUEUE_GRAPHILE_URL
                ? this.serverConfig.JOB_QUEUE_GRAPHILE_URL
                : this.serverConfig
            const onError = (error: Error) => {
                if (resolved) {
                    this.onConnectionError(error)
                } else {
                    reject(error)
                }
            }
            const pool = createPostgresPool(configOrDatabaseUrl, onError)
            try {
                await pool.query('select 1')
            } catch (error) {
                reject(error)
            }
            resolved = true
            resolve(pool)
        })
    }
}
