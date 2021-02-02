import { KAFKA_EVENTS_INGESTION_HANDOFF } from '../ingestion/topics'
import { Properties } from '@posthog/plugin-scaffold'
import { DateTime } from 'luxon'
import { PluginsServer, PluginConfig, RawEventMessage } from 'types'
import { version } from '../../package.json'
import Client from '../celery/client'
import { UUIDT } from '../utils'

interface InternalData {
    distinct_id: string
    event: string
    timestamp: string
    properties: Properties
    team_id: number
    uuid: string
}

export interface DummyPostHog {
    capture(event: string, properties?: Record<string, any>): void
}

export function createPosthog(server: PluginsServer, pluginConfig: PluginConfig): DummyPostHog {
    const distinctId = pluginConfig.plugin?.name || `plugin-id-${pluginConfig.plugin_id}`

    let sendEvent: (data: InternalData) => Promise<void>

    if (server.KAFKA_ENABLED) {
        // Sending event to our Kafka>ClickHouse pipeline
        const producer = server.kafka!.producer()
        ;(createPosthog as any).producer = producer
        sendEvent = async (data) => {
            await producer.connect()
            producer!.send({
                topic: KAFKA_EVENTS_INGESTION_HANDOFF,
                messages: [
                    {
                        key: data.uuid,
                        value: JSON.stringify({
                            distinct_id: data.distinct_id,
                            ip: '',
                            site_url: '',
                            data: JSON.stringify(data),
                            team_id: pluginConfig.team_id,
                            now: data.timestamp,
                            sent_at: data.timestamp,
                            uuid: data.uuid,
                        } as RawEventMessage),
                    },
                ],
            })
        }
    } else {
        // Sending event to our Redis>Postgres pipeline
        const client = new Client(server.redis, server.PLUGINS_CELERY_QUEUE)
        sendEvent = async (data) => {
            client.sendTask(
                'posthog.tasks.process_event.process_event_with_plugins',
                [data.distinct_id, null, null, data, pluginConfig.team_id, data.timestamp, data.timestamp],
                {}
            )
        }
    }

    return {
        capture(event, properties = {}) {
            const { timestamp = DateTime.utc().toISO(), ...otherProperties } = properties
            const data: InternalData = {
                distinct_id: distinctId,
                event,
                timestamp,
                properties: {
                    $lib: 'posthog-plugin-server',
                    $lib_version: version,
                    ...otherProperties,
                },
                team_id: pluginConfig.team_id,
                uuid: new UUIDT().toString(),
            }
            sendEvent(data)
        },
    }
}
