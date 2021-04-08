import ClickHouse from '@posthog/clickhouse'
import * as Sentry from '@sentry/node'
import * as fs from 'fs'
import { createPool } from 'generic-pool'
import { StatsD } from 'hot-shots'
import Redis from 'ioredis'
import { Kafka } from 'kafkajs'
import { DateTime } from 'luxon'
import { scheduleJob } from 'node-schedule'
import * as path from 'path'
import { types as pgTypes } from 'pg'

import { PluginsServer, PluginsServerConfig } from '../types'
import { EventsProcessor } from '../worker/ingestion/process-event'
import { defaultConfig } from './config'
import { DB } from './db'
import { KafkaProducerWrapper } from './kafka-producer-wrapper'
import { performMmdbStalenessCheck, prepareMmdb } from './mmdb'
import { status } from './status'
import { createKafka, createPostgresPool, createRedis } from './utils'

export async function createServer(
    config: Partial<PluginsServerConfig> = {},
    threadId: number | null = null
): Promise<[PluginsServer, () => Promise<void>]> {
    const serverConfig: PluginsServerConfig = {
        ...defaultConfig,
        ...config,
    }

    let statsd: StatsD | undefined
    if (serverConfig.STATSD_HOST) {
        statsd = new StatsD({
            port: serverConfig.STATSD_PORT,
            host: serverConfig.STATSD_HOST,
            prefix: serverConfig.STATSD_PREFIX,
            telegraf: true,
            errorHandler: (error) => {
                status.warn('⚠️', 'StatsD error', error)
                Sentry.captureException(error, {
                    extra: { threadId },
                })
            },
        })
        // don't repeat the same info in each thread
        if (threadId === null) {
            status.info(
                '🪵',
                `Sending metrics to StatsD at ${serverConfig.STATSD_HOST}:${serverConfig.STATSD_PORT}, prefix: "${serverConfig.STATSD_PREFIX}"`
            )
        }
    }

    let clickhouse: ClickHouse | undefined
    let kafka: Kafka | undefined
    let kafkaProducer: KafkaProducerWrapper | undefined
    if (serverConfig.KAFKA_ENABLED) {
        clickhouse = new ClickHouse({
            host: serverConfig.CLICKHOUSE_HOST,
            port: serverConfig.CLICKHOUSE_SECURE ? 8443 : 8123,
            protocol: serverConfig.CLICKHOUSE_SECURE ? 'https:' : 'http:',
            user: serverConfig.CLICKHOUSE_USER,
            password: serverConfig.CLICKHOUSE_PASSWORD || undefined,
            dataObjects: true,
            queryOptions: {
                database: serverConfig.CLICKHOUSE_DATABASE,
                output_format_json_quote_64bit_integers: false,
            },
            ca: serverConfig.CLICKHOUSE_CA
                ? fs.readFileSync(path.join(serverConfig.BASE_DIR, serverConfig.CLICKHOUSE_CA)).toString()
                : undefined,
            rejectUnauthorized: serverConfig.CLICKHOUSE_CA ? false : undefined,
        })
        await clickhouse.querying('SELECT 1') // test that the connection works

        kafka = createKafka(serverConfig)
        const producer = kafka.producer()
        await producer?.connect()

        kafkaProducer = new KafkaProducerWrapper(producer, statsd, serverConfig)
    }

    // `node-postgres` will return dates as plain JS Date objects, which will use the local timezone.
    // This converts all date fields to a proper luxon UTC DateTime and then casts them to a string
    // Unfortunately this must be done on a global object before initializing the `Pool`
    pgTypes.setTypeParser(1083 /* types.TypeId.TIME */, (timeStr) =>
        timeStr ? DateTime.fromSQL(timeStr, { zone: 'utc' }).toISO() : null
    )
    pgTypes.setTypeParser(1114 /* types.TypeId.TIMESTAMP */, (timeStr) =>
        timeStr ? DateTime.fromSQL(timeStr, { zone: 'utc' }).toISO() : null
    )
    pgTypes.setTypeParser(1184 /* types.TypeId.TIMESTAMPTZ */, (timeStr) =>
        timeStr ? DateTime.fromSQL(timeStr, { zone: 'utc' }).toISO() : null
    )

    const postgres = createPostgresPool(serverConfig)

    const redisPool = createPool<Redis.Redis>(
        {
            create: () => createRedis(serverConfig),
            destroy: async (client) => {
                await client.quit()
            },
        },
        {
            min: serverConfig.REDIS_POOL_MIN_SIZE,
            max: serverConfig.REDIS_POOL_MAX_SIZE,
            autostart: true,
        }
    )

    const db = new DB(postgres, redisPool, kafkaProducer, clickhouse, statsd)

    const server: Omit<PluginsServer, 'eventsProcessor'> = {
        ...serverConfig,
        db,
        postgres,
        redisPool,
        clickhouse,
        kafka,
        kafkaProducer,
        statsd,
        mmdb: null,
        mmdbUpdateJob: null,
        plugins: new Map(),
        pluginConfigs: new Map(),
        pluginConfigsPerTeam: new Map(),

        pluginSchedule: null,
        pluginSchedulePromises: { runEveryMinute: {}, runEveryHour: {}, runEveryDay: {} },
    }

    if (!serverConfig.DISABLE_MMDB) {
        server.mmdb = await prepareMmdb(server as PluginsServer)
        server.mmdbUpdateJob = scheduleJob(
            '0 6,18 * * *',
            async () => await performMmdbStalenessCheck(server as PluginsServer)
        )
    }

    // :TODO: This is only used on worker threads, not main
    server.eventsProcessor = new EventsProcessor(server as PluginsServer)

    const closeServer = async () => {
        server.mmdbUpdateJob?.cancel()
        if (kafkaProducer) {
            clearInterval(kafkaProducer.flushInterval)
            await kafkaProducer.flush()
            await kafkaProducer.producer.disconnect()
        }
        await redisPool.drain()
        await redisPool.clear()
        await server.postgres.end()
    }

    return [server as PluginsServer, closeServer]
}
