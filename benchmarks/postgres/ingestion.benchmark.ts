import { PluginEvent } from '@posthog/plugin-scaffold/src/types'
import { DateTime } from 'luxon'
import { performance } from 'perf_hooks'

import { IEvent } from '../../src/idl/protos'
import { EventsProcessor } from '../../src/ingestion/process-event'
import { createServer } from '../../src/server'
import { LogLevel, PluginsServer, SessionRecordingEvent, Team } from '../../src/types'
import { UUIDT } from '../../src/utils'
import { getFirstTeam, resetTestDatabase } from '../../tests/helpers/sql'
import { endLog,startLog } from '../utils'

jest.mock('../../src/sql')
jest.setTimeout(600000) // 600 sec timeout

describe('ingestion benchmarks', () => {
    let team: Team
    let server: PluginsServer
    let stopServer: () => Promise<void>
    let eventsProcessor: EventsProcessor
    let now = DateTime.utc()

    async function processOneEvent(): Promise<IEvent | SessionRecordingEvent> {
        return await eventsProcessor.processEvent(
            'my_id',
            '127.0.0.1',
            'http://localhost',
            ({
                event: 'default event',
                timestamp: now.toISO(),
                properties: { token: team.api_token },
            } as any) as PluginEvent,
            team.id,
            now,
            now,
            new UUIDT().toString()
        )
    }

    beforeEach(async () => {
        await resetTestDatabase(`
            function processEvent (event, meta) {
                event.properties["somewhere"] = "in a benchmark";
                return event
            }
        `)
        ;[server, stopServer] = await createServer({
            PLUGINS_CELERY_QUEUE: 'benchmark-plugins-celery-queue',
            CELERY_DEFAULT_QUEUE: 'benchmark-celery-default-queue',
            LOG_LEVEL: LogLevel.Log,
        })
        eventsProcessor = new EventsProcessor(server)
        team = await getFirstTeam(server)
        now = DateTime.utc()

        // warmup
        for (let i = 0; i < 5; i++) {
            await processOneEvent()
        }
    })

    afterEach(async () => {
        await stopServer?.()
    })

    test('basic sequential ingestion', async () => {
        const count = 3000

        startLog('Postgres', 'Await Ingested', 'event', 'events')

        for (let i = 0; i < count; i++) {
            await processOneEvent()
        }

        endLog(count)
    })

    test('basic parallel ingestion', async () => {
        const count = 3000
        const promises = []

        startLog('Postgres', 'Promise.all Ingested', 'event', 'events')

        for (let i = 0; i < count; i++) {
            promises.push(processOneEvent())
        }
        await Promise.all(promises)

        endLog(count)
    })
})
