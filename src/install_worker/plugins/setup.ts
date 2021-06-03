import { PluginAttachment } from '@posthog/plugin-scaffold'

import { Hub, Plugin, PluginConfig, PluginConfigId, PluginId, PluginTaskType, TeamId } from '../../types'
import { status } from '../../utils/status'
import { loadPlugin } from '../../worker/plugins/loadPlugin'
import { loadSchedule } from '../../worker/plugins/setup'
import { teardownPlugins } from '../../worker/plugins/teardown'
import { LazyPluginVM } from '../../worker/vm/lazy'

export async function setupPlugin(hub: Hub, pluginConfig: PluginConfig): Promise<void> {
    const { plugins, pluginConfigs, pluginConfigsPerTeam } = await loadPluginFromDB(hub, pluginConfig)
    const pluginVMLoadPromises: Array<Promise<any>> = []
    for (const [id, pluginConfig] of pluginConfigs) {
        const plugin = plugins.get(pluginConfig.plugin_id)
        const prevConfig = hub.pluginConfigs.get(id)
        const prevPlugin = prevConfig ? hub.plugins.get(pluginConfig.plugin_id) : null

        status.info('Setting up plugin: ', pluginConfig)
        if (
            prevConfig &&
            pluginConfig.updated_at === prevConfig.updated_at &&
            plugin?.updated_at == prevPlugin?.updated_at
        ) {
            pluginConfig.vm = prevConfig.vm
        } else {
            pluginConfig.vm = new LazyPluginVM()
            pluginVMLoadPromises.push(loadPlugin(hub, pluginConfig))

            if (prevConfig) {
                void teardownPlugins(hub, prevConfig)
            }
        }
    }

    await Promise.all(pluginVMLoadPromises)

    hub.plugins = plugins
    hub.pluginConfigs = pluginConfigs
    hub.pluginConfigsPerTeam = pluginConfigsPerTeam

    for (const teamId of hub.pluginConfigsPerTeam.keys()) {
        hub.pluginConfigsPerTeam.get(teamId)?.sort((a, b) => a.order - b.order)
    }

    void loadSchedule(hub)
}

async function loadPluginFromDB(
    server: Hub,
    pluginConfig: PluginConfig
): Promise<Pick<Hub, 'plugins' | 'pluginConfigs' | 'pluginConfigsPerTeam'>> {
    const plugin = await server.db.fetchPlugin(pluginConfig.plugin_id)
    const plugins = new Map<PluginId, Plugin>()

    if (plugin) {
        plugins.set(plugin.id, plugin)
    }

    const pluginAttachmentRows = await server.db.fetchPluginAttachments(pluginConfig.id)
    const attachmentsPerConfig = new Map<TeamId, Record<string, PluginAttachment>>()
    for (const row of pluginAttachmentRows) {
        let attachments = attachmentsPerConfig.get(row.plugin_config_id!)
        if (!attachments) {
            attachments = {}
            attachmentsPerConfig.set(row.plugin_config_id!, attachments)
        }
        attachments[row.key] = {
            content_type: row.content_type,
            file_name: row.file_name,
            contents: row.contents,
        }
    }

    const row = await server.db.fetchPluginConfig(pluginConfig.id)

    const pluginConfigs = new Map<PluginConfigId, PluginConfig>()
    const pluginConfigsPerTeam = new Map<TeamId, PluginConfig[]>()

    if (row) {
        const plugin = plugins.get(row.plugin_id)
        const pluginConfig: PluginConfig = {
            ...row,
            plugin: plugin,
            attachments: attachmentsPerConfig.get(row.id) || {},
            vm: null,
        }
        pluginConfigs.set(row.id, pluginConfig)

        if (!row.team_id) {
            console.error(`🔴 PluginConfig(id=${row.id}) without team_id!`)
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
