import * as Sentry from '@sentry/node'
import Redlock from 'redlock'

import { status } from '../../shared/status'
import { createRedis } from '../../shared/utils'
import { PluginsServer } from '../../types'

export async function startRedlock(
    server: PluginsServer,
    resource: string,
    onLock: () => Promise<void> | void,
    onUnlock: () => Promise<void> | void,
    ttl = 60
): Promise<() => Promise<void>> {
    status.info('⏰', `Starting redlock "${resource}" ...`)

    let stopped = false
    let weHaveTheLock = false
    let lock: Redlock.Lock
    let lockTimeout: NodeJS.Timeout

    const lockTTL = ttl * 1000 // 60 sec
    const retryDelay = lockTTL / 10 // 6 sec
    const extendDelay = lockTTL / 2 // 30 sec

    // use another redis connection for redlock
    const redis = await createRedis(server)

    const redlock = new Redlock([redis], {
        // we handle retires ourselves to have a way to cancel the promises on quit
        // without this, the `await redlock.lock()` code will remain inflight and cause issues
        retryCount: 0,
    })

    redlock.on('clientError', (error) => {
        if (stopped) {
            return
        }
        status.error('🔴', `Redlock "${resource}" client error occurred:\n`, error)
        Sentry.captureException(error, { extra: { resource } })
    })

    const tryToGetTheLock = async () => {
        try {
            lock = await redlock.lock(resource, lockTTL)
            weHaveTheLock = true

            status.info('🔒', `Redlock "${resource}" acquired!`)

            const extendLock = async () => {
                if (stopped) {
                    return
                }
                try {
                    lock = await lock.extend(lockTTL)
                    lockTimeout = setTimeout(extendLock, extendDelay)
                } catch (error) {
                    status.error('🔴', `Redlock cannot extend lock "${resource}":\n`, error)
                    Sentry.captureException(error, { extra: { resource } })
                    weHaveTheLock = false
                    lockTimeout = setTimeout(tryToGetTheLock, 0)
                }
            }

            lockTimeout = setTimeout(extendLock, extendDelay)

            await onLock?.()
        } catch (error) {
            if (stopped) {
                return
            }
            weHaveTheLock = false
            if (error instanceof Redlock.LockError) {
                lockTimeout = setTimeout(tryToGetTheLock, retryDelay)
            } else {
                Sentry.captureException(error, { extra: { resource } })
                status.error('🔴', `Redlock "${resource}" error:\n`, error)
            }
        }
    }

    lockTimeout = setTimeout(tryToGetTheLock, 0)

    return async () => {
        stopped = true
        lockTimeout && clearTimeout(lockTimeout)

        await lock?.unlock().catch(Sentry.captureException)
        await redis.quit()
        await onUnlock?.()
    }
}