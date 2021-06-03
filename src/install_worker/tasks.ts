import {
    Action,
    EnqueuedJob,
    Hub,
    InstallConfig,
    PluginConfig,
    PluginConfigId,
    PluginId,
    PluginTaskType,
    Team,
} from '../types'
import { status } from '../utils/status'
import { workerTasks as originalWorkerTasks } from '../worker/tasks'

type TaskRunner = (hub: Hub, args: any) => Promise<any> | any

export const workerTasks: Record<string, TaskRunner> = {
    ...originalWorkerTasks,
    installPlugin: (hub, args: { installConfig: InstallConfig }) => {
        status.info('Piscina worker finished task')
        return {}
    },
    setupPlugin: (hub, args: { installConfig: InstallConfig }) => {
        return {}
    },
    reloadPlugins: (hub) => {
        return {} // TODO(nk): figure how this should be setup
    },
    // override other tasks here
}
