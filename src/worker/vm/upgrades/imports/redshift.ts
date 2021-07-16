import { Plugin, PluginMeta } from '@posthog/plugin-scaffold'
import { Client, QueryResult } from 'pg'

import {
    Hub,
    MetricMathOperations,
    PluginConfig,
    PluginConfigVMInternalResponse,
    PluginLogEntrySource,
    PluginLogEntryType,
    PluginTaskType,
} from '../../../../types'
import { createPosthog } from './../../extensions/posthog'
import { DummyPostHog } from './../../extensions/posthog'

type RedshiftImportUpgrade = Plugin<{
    global: {
        pgClient: Client
        eventsToIgnore: Set<string>
        sanitizedTableName: string
        importAndIngestEvents: (
            payload: ImportEventsJobPayload,
            meta: PluginMeta<RedshiftImportUpgrade>
        ) => Promise<void>
        initialOffset: number
        posthog: DummyPostHog
        totalRows: number
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        eventsToIgnore: string
        orderByColumn: string
    }
}>

interface ImportEventsJobPayload extends Record<string, any> {
    offset?: number
    retriesPerformedSoFar: number
}

interface ExecuteQueryResponse {
    error: Error | null
    queryResult: QueryResult<any> | null
}

const EVENTS_PER_BATCH = 10

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_]+/g, '')
}

export function upgradeRedshiftImport(
    hub: Hub,
    pluginConfig: PluginConfig,
    response: PluginConfigVMInternalResponse<PluginMeta<RedshiftImportUpgrade>>
): void {
    const { methods, tasks, meta } = response

    const oldSetupPlugin = methods.setupPlugin
    methods.setupPlugin = async () => {
        const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
        for (const option of requiredConfigOptions) {
            if (!(option in meta.config)) {
                throw new Error(`Required config option ${option} is missing!`)
            }
        }

        if (!meta.config.clusterHost.endsWith('redshift.amazonaws.com')) {
            throw new Error('Cluster host must be a valid AWS Redshift host')
        }

        // the way this is done means we'll continuously import as the table grows
        // to only import historical data, we should set a totalRows value in storage once
        const totalRowsResult = await executeQuery(
            `SELECT COUNT(1) FROM ${sanitizeSqlIdentifier(meta.config.tableName)}`,
            [],
            meta
        )
        if (totalRowsResult.error || !totalRowsResult.queryResult) {
            throw new Error('Unable to connect to Redshift!')
        }
        meta.global.totalRows = Number(totalRowsResult.queryResult.rows[0].count)

        // we're outside the vm so need to recreate this
        meta.global.posthog = createPosthog(hub, pluginConfig)

        await oldSetupPlugin?.()

        // used for picking up where we left off after a restart
        const offset = await meta.storage.get('import_offset', 0)

        // needed to prevent race conditions around offsets leading to events ingested twice
        meta.global.initialOffset = Number(offset)
        await meta.cache.set('import_offset', Number(offset) / EVENTS_PER_BATCH)

        await meta.jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runIn(10, 'seconds')
    }

    meta.global.importAndIngestEvents = async (
        payload: ImportEventsJobPayload,
        meta: PluginMeta<RedshiftImportUpgrade>
    ) => {
        if (payload.offset && payload.retriesPerformedSoFar >= 15) {
            createLog(
                `Import error: Unable to process rows ${payload.offset}-${
                    payload.offset + EVENTS_PER_BATCH
                }. Skipped them.`,
                PluginLogEntryType.Error
            )
            return
        }

        let offset: number
        if (payload.offset) {
            offset = payload.offset
        } else {
            const redisIncrementedOffset = await meta.cache.incr('import_offset')
            offset = meta.global.initialOffset + (redisIncrementedOffset - 1) * EVENTS_PER_BATCH
        }

        if (offset > meta.global.totalRows) {
            createLog(`Done processing all rows in ${meta.config.tableName}`)
            return
        }

        // ORDER BY ${sanitizeSqlIdentifier( meta.config.orderByColumn)}
        const query = `SELECT * FROM ${sanitizeSqlIdentifier(
            meta.config.tableName
        )} OFFSET $1 LIMIT ${EVENTS_PER_BATCH};`
        const values = [offset]

        const queryResponse = await executeQuery(query, values, meta)

        if (queryResponse.error || !queryResponse.queryResult) {
            const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
            createLog(
                `Unable to process rows ${offset}-${
                    offset + EVENTS_PER_BATCH
                }. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
            )
            await meta.jobs
                .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
                .runIn(nextRetrySeconds, 'seconds')
        }

        const eventsToIngest = await methods.importEventsFromRedshift!(queryResponse.queryResult!.rows)

        if (!Array.isArray(eventsToIngest)) {
            throw new Error('importEventsFromRedshift must return an array of plugin events')
        }

        for (const event of eventsToIngest) {
            meta.global.posthog.capture(event.event, event.properties)
        }

        createLog(
            `Processed rows ${offset}-${offset + EVENTS_PER_BATCH} and ingested ${eventsToIngest.length} event${
                eventsToIngest.length > 1 ? 's' : ''
            } from them.`
        )
        incrementMetric('events_imported', eventsToIngest.length)
        await meta.jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runNow()
    }

    methods.teardownPlugin = async () => {
        const redisOffset = await meta.cache.get('import_offset', 0)
        await meta.storage.set('import_offset', Number(redisOffset) * EVENTS_PER_BATCH)
    }

    async function executeQuery(
        query: string,
        values: any[],
        meta: PluginMeta<RedshiftImportUpgrade>
    ): Promise<ExecuteQueryResponse> {
        const { config } = meta

        const pgClient = new Client({
            user: config.dbUsername,
            password: config.dbPassword,
            host: config.clusterHost,
            database: config.dbName,
            port: parseInt(config.clusterPort),
        })

        await pgClient.connect()

        let error: Error | null = null
        let queryResult: QueryResult<any> | null = null
        try {
            queryResult = await pgClient.query(query, values)
        } catch (err) {
            error = err
        }

        await pgClient.end()

        return { error, queryResult }
    }

    tasks.job['importAndIngestEvents'] = {
        name: 'importAndIngestEvents',
        type: PluginTaskType.Job,
        exec: (payload) => meta.global.importAndIngestEvents(payload as ImportEventsJobPayload, meta),
    }

    function incrementMetric(metricName: string, value: number) {
        hub.pluginMetricsManager.updateMetric({
            metricName,
            value,
            pluginConfig,
            metricOperation: MetricMathOperations.Increment,
        })
    }

    function createLog(message: string, type: PluginLogEntryType = PluginLogEntryType.Log) {
        void hub.db.queuePluginLogEntry({
            pluginConfig,
            message: `(${hub.instanceId}) ${message}`,
            source: PluginLogEntrySource.System,
            type: type,
            instanceId: hub.instanceId,
        })
    }
}
