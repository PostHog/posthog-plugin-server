import { PluginEvent } from '@posthog/plugin-scaffold/src/types'
import { mocked } from 'ts-jest/utils'

import { clearError, processError } from '../src/error'
import { runPlugins } from '../src/plugins/run'
import { loadSchedule, setupPlugins } from '../src/plugins/setup'
import { createServer } from '../src/server'
import { LogLevel, PluginsServer } from '../src/types'
import {
    mockPluginTempFolder,
    mockPluginWithArchive,
    plugin60,
    pluginAttachment1,
    pluginConfig39,
} from './helpers/plugins'
import { getPluginAttachmentRows, getPluginConfigRows, getPluginRows, setError } from './helpers/sqlMock'

jest.mock('../src/sql')
jest.mock('../src/status')
jest.mock('../src/error')

let mockServer: PluginsServer
let closeServer: () => Promise<void>
beforeEach(async () => {
    ;[mockServer, closeServer] = await createServer({ LOG_LEVEL: LogLevel.Log })
    console.warn = jest.fn() as any
})
afterEach(async () => {
    await closeServer()
})

test('setupPlugins and runPlugins', async () => {
    getPluginRows.mockReturnValueOnce([plugin60])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])

    await setupPlugins(mockServer)
    const { plugins, pluginConfigs } = mockServer

    expect(getPluginRows).toHaveBeenCalled()
    expect(getPluginAttachmentRows).toHaveBeenCalled()
    expect(getPluginConfigRows).toHaveBeenCalled()

    expect(Array.from(plugins.entries())).toEqual([[60, plugin60]])
    expect(Array.from(pluginConfigs.keys())).toEqual([39])

    const pluginConfig = pluginConfigs.get(39)!
    expect(pluginConfig.id).toEqual(pluginConfig39.id)
    expect(pluginConfig.team_id).toEqual(pluginConfig39.team_id)
    expect(pluginConfig.plugin_id).toEqual(pluginConfig39.plugin_id)
    expect(pluginConfig.enabled).toEqual(pluginConfig39.enabled)
    expect(pluginConfig.order).toEqual(pluginConfig39.order)
    expect(pluginConfig.config).toEqual(pluginConfig39.config)
    expect(pluginConfig.error).toEqual(pluginConfig39.error)

    expect(pluginConfig.plugin).toEqual(plugin60)
    expect(pluginConfig.attachments).toEqual({
        maxmindMmdb: {
            content_type: pluginAttachment1.content_type,
            file_name: pluginAttachment1.file_name,
            contents: pluginAttachment1.contents,
        },
    })
    expect(pluginConfig.vm).toBeDefined()
    const vm = await pluginConfig.vm!.promise
    expect(Object.keys(vm!.methods)).toEqual(['processEvent', 'processEventBatch'])

    expect(clearError).toHaveBeenCalledWith(mockServer, pluginConfig)

    const processEvent = vm!.methods['processEvent']
    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent
    await processEvent(event)

    expect(event.properties!['processed']).toEqual(true)

    event.properties!['processed'] = false

    const returnedEvent = await runPlugins(mockServer, event)
    expect(event.properties!['processed']).toEqual(true)
    expect(returnedEvent!.properties!['processed']).toEqual(true)
})

test('plugin returns null', async () => {
    getPluginRows.mockReturnValueOnce([mockPluginWithArchive('function processEvent (event, meta) { return null }')])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([])

    await setupPlugins(mockServer)

    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent
    const returnedEvent = await runPlugins(mockServer, event)

    expect(returnedEvent).toEqual(null)
})

test('plugin meta has what it should have', async () => {
    getPluginRows.mockReturnValueOnce([
        mockPluginWithArchive(`
            function setupPlugin (meta) { meta.global.key = 'value' }
            function processEvent (event, meta) { event.properties=meta; return event }
        `),
    ])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)

    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent
    const returnedEvent = await runPlugins(mockServer, event)

    expect(Object.keys(returnedEvent!.properties!).sort()).toEqual([
        'attachments',
        'cache',
        'config',
        'global',
        'storage',
    ])
    expect(returnedEvent!.properties!['attachments']).toEqual({
        maxmindMmdb: { content_type: 'application/octet-stream', contents: Buffer.from('test'), file_name: 'test.txt' },
    })
    expect(returnedEvent!.properties!['config']).toEqual({ localhostIP: '94.224.212.175' })
    expect(returnedEvent!.properties!['global']).toEqual({ key: 'value' })
})

test('archive plugin with broken index.js does not do much', async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    getPluginRows.mockReturnValueOnce([
        mockPluginWithArchive(`
            function setupPlugin (met
        `),
    ])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    const pluginConfig = pluginConfigs.get(39)!
    expect(await pluginConfig.vm!.promise).toEqual(null)

    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent
    const returnedEvent = await runPlugins(mockServer, { ...event })
    expect(returnedEvent).toEqual(event)

    expect(processError).toHaveBeenCalledWith(mockServer, pluginConfig, expect.any(SyntaxError))
    const error = mocked(processError).mock.calls[0][2]! as Error
    expect(error.message).toContain(': Unexpected token, expected ","')
})

test('local plugin with broken index.js does not do much', async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    const [plugin, unlink] = mockPluginTempFolder(`function setupPlugin (met`)
    getPluginRows.mockReturnValueOnce([plugin])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    const pluginConfig = pluginConfigs.get(39)!
    expect(await pluginConfig.vm!.promise).toEqual(null)

    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent
    const returnedEvent = await runPlugins(mockServer, { ...event })
    expect(returnedEvent).toEqual(event)

    expect(processError).toHaveBeenCalledWith(mockServer, pluginConfig, expect.any(SyntaxError))
    const error = mocked(processError).mock.calls[0][2]! as Error
    expect(error.message).toContain(': Unexpected token, expected ","')

    unlink()
})

test('archive plugin with broken plugin.json does not do much', async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    getPluginRows.mockReturnValueOnce([
        mockPluginWithArchive(
            `function processEvent (event, meta) { event.properties.processed = true; return event }`,
            '{ broken: "plugin.json" -=- '
        ),
    ])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    expect(processError).toHaveBeenCalledWith(
        mockServer,
        pluginConfigs.get(39)!,
        'Can not load plugin.json for plugin "test-maxmind-plugin"'
    )
    expect(pluginConfigs.get(39)!.vm).toEqual(null)
})

test('local plugin with broken plugin.json does not do much', async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    const [plugin, unlink] = mockPluginTempFolder(
        `function processEvent (event, meta) { event.properties.processed = true; return event }`,
        '{ broken: "plugin.json" -=- '
    )
    getPluginRows.mockReturnValueOnce([plugin])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    expect(processError).toHaveBeenCalledWith(
        mockServer,
        pluginConfigs.get(39)!,
        expect.stringContaining('Could not load posthog config at ')
    )
    expect(pluginConfigs.get(39)!.vm).toEqual(null)

    unlink()
})

test('plugin with http urls must have an archive', async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    getPluginRows.mockReturnValueOnce([{ ...plugin60, archive: null }])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    expect(pluginConfigs.get(39)!.plugin!.url).toContain('https://')
    expect(processError).toHaveBeenCalledWith(
        mockServer,
        pluginConfigs.get(39)!,
        'Un-downloaded remote plugins not supported! Plugin: "test-maxmind-plugin"'
    )
    expect(pluginConfigs.get(39)!.vm).toEqual(null)
})

test("plugin with broken archive doesn't load", async () => {
    // silence some spam
    console.log = jest.fn()
    console.error = jest.fn()

    getPluginRows.mockReturnValueOnce([{ ...plugin60, archive: Buffer.from('this is not a zip') }])
    getPluginConfigRows.mockReturnValueOnce([pluginConfig39])
    getPluginAttachmentRows.mockReturnValueOnce([pluginAttachment1])

    await setupPlugins(mockServer)
    const { pluginConfigs } = mockServer

    expect(pluginConfigs.get(39)!.plugin!.url).toContain('https://')
    expect(processError).toHaveBeenCalledWith(
        mockServer,
        pluginConfigs.get(39)!,
        Error('Could not read archive as .zip or .tgz')
    )
    expect(pluginConfigs.get(39)!.vm).toEqual(null)
})

test('plugin config order', async () => {
    const setOrderCode = (id: number) => {
        return `
            function processEvent(event) {
                if (!event.properties.plugins) { event.properties.plugins = [] }
                event.properties.plugins.push(${id})
                return event
            }
        `
    }

    getPluginRows.mockReturnValueOnce([
        { ...plugin60, id: 60, plugin_type: 'source', archive: null, source: setOrderCode(60) },
        { ...plugin60, id: 61, plugin_type: 'source', archive: null, source: setOrderCode(61) },
        { ...plugin60, id: 62, plugin_type: 'source', archive: null, source: setOrderCode(62) },
    ])
    getPluginAttachmentRows.mockReturnValueOnce([])
    getPluginConfigRows.mockReturnValueOnce([
        { ...pluginConfig39, order: 2 },
        { ...pluginConfig39, plugin_id: 61, id: 40, order: 1 },
        { ...pluginConfig39, plugin_id: 62, id: 41, order: 3 },
    ])

    await setupPlugins(mockServer)
    const { pluginConfigsPerTeam } = mockServer

    expect(pluginConfigsPerTeam.get(pluginConfig39.team_id)?.map((c) => [c.id, c.order])).toEqual([
        [40, 1],
        [39, 2],
        [41, 3],
    ])

    const event = { event: '$test', properties: {}, team_id: 2 } as PluginEvent

    const returnedEvent1 = await runPlugins(mockServer, { ...event, properties: { ...event.properties } })
    expect(returnedEvent1!.properties!.plugins).toEqual([61, 60, 62])

    const returnedEvent2 = await runPlugins(mockServer, { ...event, properties: { ...event.properties } })
    expect(returnedEvent2!.properties!.plugins).toEqual([61, 60, 62])
})

describe('loadSchedule()', () => {
    const mockConfig = (tasks: any) => ({ vm: { getTasks: () => Promise.resolve(tasks) } })

    const mockServer = {
        pluginConfigs: new Map(
            Object.entries({
                1: {},
                2: mockConfig({ runEveryMinute: null, runEveryHour: () => 123 }),
                3: mockConfig({ runEveryMinute: () => 123, foo: () => 'bar' }),
            })
        ),
    } as any

    it('sets server.pluginSchedule once all plugins are ready', async () => {
        const promise = loadSchedule(mockServer)
        expect(mockServer.pluginSchedule).toEqual(null)

        await promise

        expect(mockServer.pluginSchedule).toEqual({
            runEveryMinute: ['3'],
            runEveryHour: ['2'],
            runEveryDay: [],
        })
    })
})
