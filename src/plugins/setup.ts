import { PluginAttachment } from '@posthog/plugin-scaffold'

import { getPluginAttachmentRows, getPluginConfigRows, getPluginRows } from '../sql'
import { status } from '../status'
import { Plugin, PluginConfig, PluginConfigId, PluginId, PluginsServer, TeamId } from '../types'
import { loadPlugin } from './loadPlugin'

export async function setupPlugins(server: PluginsServer): Promise<void> {
    const { plugins, pluginConfigs, pluginConfigsPerTeam } = await loadPluginsFromDB(server)

    for (const [id, pluginConfig] of pluginConfigs) {
        const plugin = plugins.get(pluginConfig.plugin_id)
        const prevConfig = server.pluginConfigs.get(id)
        const prevPlugin = prevConfig ? server.plugins.get(pluginConfig.plugin_id) : null

        // :TRICKY: This forces a reload for plugin VMs which have either been added or changedW
        if (
            prevConfig &&
            pluginConfig.updated_at === prevConfig.updated_at &&
            plugin?.updated_at == prevPlugin?.updated_at
        ) {
            pluginConfig.vm = prevConfig.vm
        }

        if (!pluginConfig.vm) {
            await loadPlugin(server, pluginConfig)
        }
    }

    server.plugins = plugins
    server.pluginConfigs = pluginConfigs
    server.pluginConfigsPerTeam = pluginConfigsPerTeam

    for (const teamId of server.pluginConfigsPerTeam.keys()) {
        server.pluginConfigsPerTeam.get(teamId)?.sort((a, b) => a.order - b.order)
    }

    void loadSchedule(server)
}

async function loadPluginsFromDB(
    server: PluginsServer
): Promise<Pick<PluginsServer, 'plugins' | 'pluginConfigs' | 'pluginConfigsPerTeam'>> {
    const pluginRows = await getPluginRows(server)
    const plugins = new Map<PluginId, Plugin>()

    for (const row of pluginRows) {
        plugins.set(row.id, row)
    }

    const pluginAttachmentRows = await getPluginAttachmentRows(server)
    const attachmentsPerConfig = new Map<TeamId, Record<string, PluginAttachment>>()
    for (const row of pluginAttachmentRows) {
        let attachments = attachmentsPerConfig.get(row.plugin_config_id)
        if (!attachments) {
            attachments = {}
            attachmentsPerConfig.set(row.plugin_config_id, attachments)
        }
        attachments[row.key] = {
            content_type: row.content_type,
            file_name: row.file_name,
            contents: row.contents,
        }
    }

    const pluginConfigRows = await getPluginConfigRows(server)
    const foundPluginConfigs = new Map<number, boolean>()

    const pluginConfigs = new Map<PluginConfigId, PluginConfig>()
    const pluginConfigsPerTeam = new Map<TeamId, PluginConfig[]>()

    for (const row of pluginConfigRows) {
        const plugin = server.plugins.get(row.plugin_id)
        if (!plugin) {
            continue
        }
        foundPluginConfigs.set(row.id, true)
        const pluginConfig: PluginConfig = {
            ...row,
            plugin: plugin,
            attachments: attachmentsPerConfig.get(row.id) || {},
            vm: null,
        }
        pluginConfigs.set(row.id, pluginConfig)

        if (!row.team_id) {
            console.error(`🔴 PluginConfig(id=${row.id}) without team_id!`)
            continue
        }

        let teamConfigs = pluginConfigsPerTeam.get(row.team_id)
        if (!teamConfigs) {
            teamConfigs = []
            pluginConfigsPerTeam.set(row.team_id, teamConfigs)
        }
        teamConfigs.push(pluginConfig)
    }

    return { plugins, pluginConfigs, pluginConfigsPerTeam }
}

export async function loadSchedule(server: PluginsServer): Promise<void> {
    server.pluginSchedule = null

    // gather runEvery* tasks into a schedule
    const pluginSchedule: Record<string, PluginConfigId[]> = { runEveryMinute: [], runEveryHour: [], runEveryDay: [] }

    for (const [id, pluginConfig] of server.pluginConfigs) {
        const tasks = (await pluginConfig.vm?.getTasks()) ?? {}
        for (const [taskName, task] of Object.entries(tasks)) {
            if (task && taskName in pluginSchedule) {
                pluginSchedule[taskName].push(id)
            }
        }
    }

    status.info('🔌', 'Finished loading plugin scheduled tasks')

    server.pluginSchedule = pluginSchedule
}
