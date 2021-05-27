import { PluginEvent } from '@posthog/plugin-scaffold/src/types'
import IORedis from 'ioredis'
import { mocked } from 'ts-jest/utils'

import { ServerInstance, startPluginsServer } from '../../src/main/pluginsServer'
import { loadPluginSchedule } from '../../src/main/services/schedule'
import { Hub, LogLevel } from '../../src/types'
import { Client } from '../../src/utils/celery/client'
import { createHub } from '../../src/utils/db/hub'
import { KafkaProducerWrapper } from '../../src/utils/db/kafka-producer-wrapper'
import { delay, UUIDT } from '../../src/utils/utils'
import { ActionManager } from '../../src/worker/ingestion/action-manager'
import { ActionMatcher } from '../../src/worker/ingestion/action-matcher'
import { ingestEvent } from '../../src/worker/ingestion/ingest-event'
import { makePiscina } from '../../src/worker/piscina'
import { runPluginTask, runProcessEvent, runProcessEventBatch } from '../../src/worker/plugins/run'
import { loadSchedule, setupPlugins } from '../../src/worker/plugins/setup'
import { teardownPlugins } from '../../src/worker/plugins/teardown'
import { createTaskRunner } from '../../src/worker/worker'
import { resetTestDatabase } from '../helpers/sql'
import { setupPiscina } from '../helpers/worker'

jest.mock('../../src/worker/ingestion/action-manager')
jest.mock('../../src/utils/db/sql')
jest.mock('../../src/utils/status')
jest.mock('../../src/worker/ingestion/ingest-event')
jest.mock('../../src/worker/plugins/run')
jest.mock('../../src/worker/plugins/setup')
jest.mock('../../src/worker/plugins/teardown')
jest.setTimeout(600000) // 600 sec timeout

function createEvent(index = 0): PluginEvent {
    return {
        distinct_id: 'my_id',
        ip: '127.0.0.1',
        site_url: 'http://localhost',
        team_id: 2,
        now: new Date().toISOString(),
        event: 'default event',
        properties: { key: 'value', index },
    }
}

beforeEach(() => {
    console.debug = jest.fn()
    jest.spyOn(ActionManager.prototype, 'reloadAllActions')
    jest.spyOn(ActionManager.prototype, 'reloadAction')
    jest.spyOn(ActionManager.prototype, 'dropAction')
    jest.spyOn(ActionMatcher.prototype, 'match')
})

test('piscina worker test', async () => {
    const workerThreads = 2
    const testCode = `
        function processEvent (event, meta) {
            event.properties["somewhere"] = "over the rainbow";
            return event
        }
        async function runEveryDay (meta) {
            return 4
        }
    `
    await resetTestDatabase(testCode)
    const piscina = setupPiscina(workerThreads, 10)

    const processEvent = (event: PluginEvent) => piscina.runTask({ task: 'processEvent', args: { event } })
    const processEventBatch = (batch: PluginEvent[]) => piscina.runTask({ task: 'processEventBatch', args: { batch } })
    const runEveryDay = (pluginConfigId: number) => piscina.runTask({ task: 'runEveryDay', args: { pluginConfigId } })
    const ingestEvent = (event: PluginEvent) => piscina.runTask({ task: 'ingestEvent', args: { event } })

    const pluginSchedule = await loadPluginSchedule(piscina)
    expect(pluginSchedule).toEqual({ runEveryDay: [39], runEveryHour: [], runEveryMinute: [] })

    const event = await processEvent(createEvent())
    expect(event.properties['somewhere']).toBe('over the rainbow')

    const eventBatch = await processEventBatch([createEvent()])
    expect(eventBatch[0]!.properties['somewhere']).toBe('over the rainbow')

    const everyDayReturn = await runEveryDay(39)
    expect(everyDayReturn).toBe(4)

    const ingestResponse1 = await ingestEvent(createEvent())
    expect(ingestResponse1).toEqual({ error: 'Not a valid UUID: "undefined"' })

    const ingestResponse2 = await ingestEvent({ ...createEvent(), uuid: new UUIDT().toString() })
    expect(ingestResponse2).toEqual({ success: true })

    await delay(2000)
    await piscina.destroy()
})

test('assume that the workerThreads and tasksPerWorker values behave as expected', async () => {
    const workerThreads = 2
    const tasksPerWorker = 3
    const testCode = `
        async function processEvent (event, meta) {
            await new Promise(resolve => __jestSetTimeout(resolve, 300))
            return event
        }
    `
    await resetTestDatabase(testCode)
    const piscina = setupPiscina(workerThreads, tasksPerWorker)
    const processEvent = (event: PluginEvent) => piscina.runTask({ task: 'processEvent', args: { event } })
    const promises: Array<Promise<any>> = []

    // warmup 2x
    await Promise.all([processEvent(createEvent()), processEvent(createEvent())])

    // process 10 events in parallel and ignore the result
    for (let i = 0; i < 10; i++) {
        promises.push(processEvent(createEvent(i)))
    }
    await delay(100)
    expect(piscina.queueSize).toBe(10 - workerThreads * tasksPerWorker)
    expect(piscina.completed).toBe(0 + 2)
    await delay(300)
    expect(piscina.queueSize).toBe(0)
    expect(piscina.completed).toBe(workerThreads * tasksPerWorker + 2)
    await delay(300)
    expect(piscina.queueSize).toBe(0)
    expect(piscina.completed).toBe(10 + 2)

    try {
        await piscina.destroy()
    } catch {}
})

describe('queue logic', () => {
    let pluginsServer: ServerInstance
    let redis: IORedis.Redis

    beforeEach(async () => {
        const testCode = `
            async function processEvent (event) {
                await new Promise(resolve => __jestSetTimeout(resolve, 1000))
                return event
            }
        `
        await resetTestDatabase(testCode)
        pluginsServer = await startPluginsServer(
            {
                WORKER_CONCURRENCY: 2,
                TASKS_PER_WORKER: 2,
                REDIS_POOL_MIN_SIZE: 3,
                REDIS_POOL_MAX_SIZE: 3,
                PLUGINS_CELERY_QUEUE: `test-plugins-celery-queue-${new UUIDT()}`,
                CELERY_DEFAULT_QUEUE: `test-celery-default-queue-${new UUIDT()}`,
                LOG_LEVEL: LogLevel.Debug,
            },
            makePiscina
        )

        redis = await pluginsServer.hub.redisPool.acquire()

        await redis.del(pluginsServer.hub.PLUGINS_CELERY_QUEUE)
        await redis.del(pluginsServer.hub.CELERY_DEFAULT_QUEUE)
    })

    afterEach(async () => {
        // :TRICKY: Ignore errors when stopping workers.
        try {
            await pluginsServer.hub.redisPool.release(redis)
            await pluginsServer.stop()
        } catch {}
    })

    it('pauses the queue if too many tasks', async () => {
        const args = Object.values({
            distinct_id: 'my-id',
            ip: '127.0.0.1',
            site_url: 'http://localhost',
            data: {
                event: '$pageview',
                properties: {},
            },
            team_id: 2,
            now: new Date().toISOString(),
            sent_at: new Date().toISOString(),
            uuid: new UUIDT().toString(),
        })

        await delay(1000)
        const baseCompleted = pluginsServer.piscina.completed

        expect(pluginsServer.piscina.queueSize).toBe(0)

        const client = new Client(pluginsServer.hub.db, pluginsServer.hub.PLUGINS_CELERY_QUEUE)
        for (let i = 0; i < 2; i++) {
            client.sendTask('posthog.tasks.process_event.process_event_with_plugins', args, {})
        }

        await delay(100)

        expect(pluginsServer.piscina.queueSize).toBe(0)
        expect(pluginsServer.piscina.completed).toBe(baseCompleted)
        expect(pluginsServer.queue.isPaused()).toBe(false)

        await delay(5000)

        expect(pluginsServer.piscina.queueSize).toBe(0)
        // 2 x (processEvent + onEvent + ingestEvent + matchActions)
        expect(pluginsServer.piscina.completed).toBe(baseCompleted + 2 * 4)
        expect(pluginsServer.queue.isPaused()).toBe(false)

        // 2 tasks * 2 threads = 4 active
        // 2 threads * 2 threads = 4 queue excess
        for (let i = 0; i < 50; i++) {
            client.sendTask('posthog.tasks.process_event.process_event_with_plugins', args, {})
        }

        let celerySize = 50,
            pausedTimes = 0
        const startTime = pluginsServer.piscina.duration
        while (celerySize > 0 || pluginsServer.piscina.queueSize > 0) {
            await delay(50)
            celerySize = await redis.llen(pluginsServer.hub.PLUGINS_CELERY_QUEUE)

            if (pluginsServer.queue.isPaused()) {
                pausedTimes++
                expect(pluginsServer.piscina.queueSize).toBeGreaterThan(0)
            }
        }

        await delay(3000)

        expect(pausedTimes).toBeGreaterThanOrEqual(10)
        expect(pluginsServer.queue.isPaused()).toBe(false)
        expect(pluginsServer.piscina.queueSize).toBe(0)
        expect(pluginsServer.piscina.completed).toEqual(baseCompleted + 156)

        const duration = pluginsServer.piscina.duration - startTime
        const expectedTimeMs = (50 / 4) * 1000

        expect(duration).toBeGreaterThanOrEqual(expectedTimeMs)
        expect(duration).toBeLessThanOrEqual(expectedTimeMs * 1.4)
    })
})

describe('createTaskRunner()', () => {
    let taskRunner: any
    let hub: Hub
    let closeHub: () => Promise<void>

    beforeEach(async () => {
        ;[hub, closeHub] = await createHub()
        taskRunner = createTaskRunner(hub)
    })
    afterEach(async () => {
        await closeHub()
    })

    it('handles `processEvent` task', async () => {
        mocked(runProcessEvent).mockReturnValue('runProcessEvent response' as any)

        expect(await taskRunner({ task: 'processEvent', args: { event: 'someEvent' } })).toEqual(
            'runProcessEvent response'
        )

        expect(runProcessEvent).toHaveBeenCalledWith(hub, 'someEvent')
    })

    it('handles `processEventBatch` task', async () => {
        mocked(runProcessEventBatch).mockReturnValue(['runProcessEventBatch response'] as any)

        expect(await taskRunner({ task: 'processEventBatch', args: { batch: 'someBatch' } })).toEqual([
            'runProcessEventBatch response',
        ])

        expect(runProcessEventBatch).toHaveBeenCalledWith(hub, 'someBatch')
    })

    it('handles `getPluginSchedule` task', async () => {
        hub.pluginSchedule = { runEveryDay: [66] }

        expect(await taskRunner({ task: 'getPluginSchedule' })).toEqual(hub.pluginSchedule)
    })

    it('handles `ingestEvent` task', async () => {
        mocked(ingestEvent).mockReturnValue('ingestEvent response' as any)

        expect(await taskRunner({ task: 'ingestEvent', args: { event: 'someEvent' } })).toEqual('ingestEvent response')

        expect(ingestEvent).toHaveBeenCalledWith(hub, 'someEvent')
    })

    it('handles `ingestEvent` task', async () => {
        mocked(ingestEvent).mockReturnValue('ingestEvent response' as any)

        expect(await taskRunner({ task: 'ingestEvent', args: { event: 'someEvent' } })).toEqual('ingestEvent response')

        expect(ingestEvent).toHaveBeenCalledWith(hub, 'someEvent')
    })

    it('handles `runEvery` tasks', async () => {
        mocked(runPluginTask).mockImplementation((server, task, taskType, pluginId) =>
            Promise.resolve(`${task} for ${pluginId}`)
        )

        expect(await taskRunner({ task: 'runEveryMinute', args: { pluginConfigId: 1 } })).toEqual(
            'runEveryMinute for 1'
        )
        expect(await taskRunner({ task: 'runEveryHour', args: { pluginConfigId: 1 } })).toEqual('runEveryHour for 1')
        expect(await taskRunner({ task: 'runEveryDay', args: { pluginConfigId: 1 } })).toEqual('runEveryDay for 1')
    })

    it('handles `reloadPlugins` task', async () => {
        await taskRunner({ task: 'reloadPlugins' })

        expect(setupPlugins).toHaveBeenCalled()
    })

    it('handles `reloadSchedule` task', async () => {
        await taskRunner({ task: 'reloadSchedule' })

        expect(loadSchedule).toHaveBeenCalled()
    })

    it('handles `matchActions` task', async () => {
        const dummyEvent = createEvent()

        await taskRunner({ task: 'matchActions', args: { event: dummyEvent } })

        expect(hub.actionMatcher.match).toHaveBeenCalledWith(dummyEvent)
    })

    it('handles `reloadAllActions` task', async () => {
        await taskRunner({ task: 'reloadAllActions' })

        expect(hub.actionMatcher.reloadAllActions).toHaveBeenCalledWith()
    })

    it('handles `reloadAction` task', async () => {
        await taskRunner({ task: 'reloadAction', args: { teamId: 2, actionId: 777 } })

        expect(hub.actionMatcher.reloadAction).toHaveBeenCalledWith(2, 777)
    })

    it('handles `dropAction` task', async () => {
        await taskRunner({ task: 'dropAction', args: { teamId: 2, actionId: 777 } })

        expect(hub.actionMatcher.dropAction).toHaveBeenCalledWith(2, 777)
    })

    it('handles `teardownPlugin` task', async () => {
        await taskRunner({ task: 'teardownPlugins' })

        expect(teardownPlugins).toHaveBeenCalled()
    })

    it('handles `flushKafkaMessages` task', async () => {
        hub.kafkaProducer = ({ flush: jest.fn() } as unknown) as KafkaProducerWrapper

        await taskRunner({ task: 'flushKafkaMessages' })

        expect(hub.kafkaProducer.flush).toHaveBeenCalled()
    })
})
