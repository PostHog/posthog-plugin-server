import { TaskQueue } from '@posthog/piscina/src/common'

import { PluginsServerConfig } from '../types'
import { status } from '../utils/status'

// Copy From: node_modules/piscina/src/index.ts -- copied because it's not exported
export interface PiscinaOptions {
    filename?: string | null
    minThreads?: number
    maxThreads?: number
    idleTimeout?: number
    maxQueue?: number | 'auto'
    concurrentTasksPerWorker?: number
    useAtomics?: boolean
    resourceLimits?: any
    argv?: string[]
    execArgv?: string[]
    env?: any
    workerData?: any
    taskQueue?: TaskQueue
    niceIncrement?: number
    trackUnmanagedFds?: boolean
}

export function createConfig(serverConfig: PluginsServerConfig, filename: string): PiscinaOptions {
    const config: PiscinaOptions = {
        filename,
        workerData: { serverConfig },
        resourceLimits: {
            stackSizeMb: 10,
        },
    }

    status.info('Creating config for 2nd piscina pool', filename)

    config.minThreads = 1
    config.maxThreads = 1

    return config
}
