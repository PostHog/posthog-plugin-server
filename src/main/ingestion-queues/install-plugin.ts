import Piscina from '@posthog/piscina'
import * as Sentry from '@sentry/node'

import { Hub, InstallConfig } from '../../types'
import { timeoutGuard } from '../../utils/db/utils'
import { status } from '../../utils/status'

export async function installPlugin(
    server: Hub,
    piscina: Piscina,
    installConfig: InstallConfig,
    checkAndPause?: () => void // pause incoming messages if we are slow in getting them out again
): Promise<void> {
    const eachEventStartTimer = new Date()

    function sendInstallTask(installConfig: InstallConfig) {
        status.info('sending task to piscina', installConfig)
        return piscina.runTask({ task: 'installPlugin', args: { installConfig } })
    }

    status.info('Running install plugin task!', installConfig)
    checkAndPause?.()

    // TODO(nk): leaving things here for now, but next steps:
    /*
        1. [x] Installation id as part of pluginConfig
        2. [x] setup new piscina pool to be used here for installPlugin task
        3. [x] Trickle down, fix everything on those "test_worker"s
        4. [x] Trickle back up, figure out how to send results back via Celery
            4.1: consider making the queue separate, if very different things
        5. [ ] Do the Django Tango
    */
    // const response = await sendInstallTask(pluginConfig)

    const response = await runInstrumentedFunction({
        server,
        installConfig,
        func: sendInstallTask,
        statsKey: 'install_queue.single_plugin',
        timeoutMessage: 'Still installing plugin. Timeout warning after 30 sec!',
    })

    checkAndPause?.()
    status.info('Response for task: ', response)
    return response
    // TODO(nk): what should be the return type? PluginConfig? or PluginError? or just success / {error}?
}

// TODO(nk): No need to dupe this?
async function runInstrumentedFunction({
    server,
    timeoutMessage,
    installConfig,
    func,
    statsKey,
}: {
    server: Hub
    installConfig: InstallConfig
    timeoutMessage: string
    statsKey: string
    func: (installConfig: InstallConfig) => Promise<any>
}): Promise<any> {
    const timeout = timeoutGuard(timeoutMessage, {
        installConfig: JSON.stringify(installConfig),
    })
    const timer = new Date()
    try {
        return await func(installConfig)
    } catch (error) {
        status.info('🔔', error)
        Sentry.captureException(error)
        throw error
    } finally {
        server.statsd?.timing(statsKey, timer)
        clearTimeout(timeout)
    }
}
