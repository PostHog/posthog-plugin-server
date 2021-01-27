import { PluginEvent, Properties } from '@posthog/plugin-scaffold'
import { DateTime, Duration } from 'luxon'
import { PluginsServer, Element, Team, Person, PersonDistinctId, CohortPeople, SessionRecordingEvent } from '../types'
import { castTimestampOrNow, UUIDT } from '../utils'
import { Event as EventProto, IEvent } from '../idl/protos'
import { Producer } from 'kafkajs'
import { KAFKA_EVENTS, KAFKA_SESSION_RECORDING_EVENTS } from './topics'
import { sanitizeEventName, elementsToString } from './utils'
import { ClickHouse } from 'clickhouse'
import { DB } from '../db'
import { status } from '../status'
import * as Sentry from '@sentry/node'

export class EventsProcessor {
    pluginsServer: PluginsServer
    db: DB
    clickhouse: ClickHouse
    kafkaProducer: Producer

    constructor(pluginsServer: PluginsServer) {
        this.pluginsServer = pluginsServer
        this.db = pluginsServer.db
        this.clickhouse = pluginsServer.clickhouse!
        this.kafkaProducer = pluginsServer.kafkaProducer!
    }

    public async processEvent(
        distinctId: string,
        ip: string,
        siteUrl: string,
        data: PluginEvent,
        teamId: number,
        now: DateTime,
        sentAt: DateTime | null,
        eventUuid: string
    ): Promise<IEvent | SessionRecordingEvent> {
        const singleSaveTimer = new Date()

        const properties: Properties = data.properties ?? {}
        if (data['$set']) {
            properties['$set'] = data['$set']
        }
        if (data['$set_once']) {
            properties['$set_once'] = data['$set_once']
        }

        const personUuid = new UUIDT().toString()

        const ts = this.handleTimestamp(data, now, sentAt)
        this.handleIdentifyOrAlias(data['event'], properties, distinctId, teamId)

        let result: IEvent | SessionRecordingEvent

        if (data['event'] === '$snapshot') {
            result = await this.createSessionRecordingEvent(
                eventUuid,
                teamId,
                distinctId,
                properties['$session_id'],
                ts,
                properties['$snapshot_data']
            )
            this.pluginsServer.statsd?.timing('kafka_queue.single_save.snapshot', singleSaveTimer)
        } else {
            result = await this.captureEE(
                eventUuid,
                personUuid,
                ip,
                siteUrl,
                teamId,
                data['event'],
                distinctId,
                properties,
                ts,
                sentAt
            )
            this.pluginsServer.statsd?.timing('kafka_queue.single_save.standard', singleSaveTimer)
        }

        return result
    }

    private handleTimestamp(data: PluginEvent, now: DateTime, sentAt: DateTime | null): DateTime {
        if (data['timestamp']) {
            if (sentAt) {
                // sent_at - timestamp == now - x
                // x = now + (timestamp - sent_at)
                try {
                    // timestamp and sent_at must both be in the same format: either both with or both without timezones
                    // otherwise we can't get a diff to add to now
                    return now.plus(DateTime.fromISO(data['timestamp']).diff(sentAt))
                } catch (error) {
                    status.error('⚠️', 'Error when handling timestamp:', error)
                    Sentry.captureException(error)
                }
            }
            return DateTime.fromISO(data['timestamp'])
        }
        if (data['offset']) {
            return now.minus(Duration.fromMillis(data['offset']))
        }
        return now
    }

    private async handleIdentifyOrAlias(
        event: string,
        properties: Properties,
        distinctId: string,
        teamId: number
    ): Promise<void> {
        if (event === '$create_alias') {
            await this.alias(properties['alias'], distinctId, teamId)
        } else if (event === '$identify') {
            if (properties['$anon_distinct_id']) {
                await this.alias(properties['$anon_distinct_id'], distinctId, teamId)
            }
            if (properties['$set'] || properties['$set_once']) {
                this.updatePersonProperties(teamId, distinctId, properties['$set'] || {}, properties['$set_once'] || {})
            }
            this.setIsIdentified(teamId, distinctId)
        }
    }

    private async setIsIdentified(teamId: number, distinctId: string, isIdentified = true): Promise<void> {
        let personFound = await this.db.fetchPerson(teamId, distinctId)
        if (!personFound) {
            try {
                const personCreated = await this.db.createPerson(
                    DateTime.utc(),
                    {},
                    teamId,
                    null,
                    true,
                    new UUIDT().toString()
                )
                this.db.addDistinctId(personCreated, distinctId)
            } catch {
                // Catch race condition where in between getting and creating,
                // another request already created this person
                personFound = await this.db.fetchPerson(teamId, distinctId)
            }
        }
        if (personFound && !personFound.is_identified) {
            await this.db.updatePerson(personFound, { is_identified: isIdentified })
        }
    }

    private async updatePersonProperties(
        teamId: number,
        distinctId: string,
        properties: Properties,
        propertiesOnce: Properties
    ): Promise<Person> {
        let personFound = await this.db.fetchPerson(teamId, distinctId)
        if (!personFound) {
            try {
                const personCreated = await this.db.createPerson(
                    DateTime.utc(),
                    properties,
                    teamId,
                    null,
                    false,
                    new UUIDT().toString()
                )
                await this.db.addDistinctId(personCreated, distinctId)
            } catch {
                // Catch race condition where in between getting and creating,
                // another request already created this person
                personFound = await this.db.fetchPerson(teamId, distinctId)
            }
        }
        const updatedProperties: Properties = { ...propertiesOnce, ...personFound!.properties, ...properties }
        return await this.db.updatePerson(personFound!, { properties: updatedProperties })
    }

    private async alias(
        previousDistinctId: string,
        distinctId: string,
        teamId: number,
        retryIfFailed = true
    ): Promise<void> {
        const oldPerson = await this.db.fetchPerson(teamId, previousDistinctId)
        const newPerson = await this.db.fetchPerson(teamId, distinctId)

        if (oldPerson && !newPerson) {
            try {
                this.db.addDistinctId(oldPerson, distinctId)
                // Catch race case when somebody already added this distinct_id between .get and .addDistinctId
            } catch {
                // integrity error
                if (retryIfFailed) {
                    // run everything again to merge the users if needed
                    this.alias(previousDistinctId, distinctId, teamId, false)
                }
            }
            return
        }

        if (!oldPerson && newPerson) {
            try {
                this.db.addDistinctId(newPerson, previousDistinctId)
                // Catch race case when somebody already added this distinct_id between .get and .addDistinctId
            } catch {
                // integrity error
                if (retryIfFailed) {
                    // run everything again to merge the users if needed
                    this.alias(previousDistinctId, distinctId, teamId, false)
                }
            }
            return
        }

        if (!oldPerson && !newPerson) {
            try {
                const personCreated = await this.db.createPerson(
                    DateTime.utc(),
                    {},
                    teamId,
                    null,
                    false,
                    new UUIDT().toString()
                )
                this.db.addDistinctId(personCreated, distinctId)
                this.db.addDistinctId(personCreated, previousDistinctId)
            } catch {
                // Catch race condition where in between getting and creating,
                // another request already created this person
                if (retryIfFailed) {
                    // Try once more, probably one of the two persons exists now
                    this.alias(previousDistinctId, distinctId, teamId, false)
                }
            }
            return
        }

        if (oldPerson && newPerson && oldPerson.id !== newPerson.id) {
            this.mergePeople(newPerson, [oldPerson])
        }
    }

    private async mergePeople(mergeInto: Person, peopleToMerge: Person[]): Promise<void> {
        let firstSeen = mergeInto.created_at

        // merge the properties
        for (const otherPerson of peopleToMerge) {
            mergeInto.properties = { ...otherPerson.properties, ...mergeInto.properties }
            if (otherPerson.created_at < firstSeen) {
                // Keep the oldest created_at (i.e. the first time we've seen this person)
                firstSeen = otherPerson.created_at
            }
        }

        await this.db.updatePerson(mergeInto, { created_at: firstSeen })

        // merge the distinct_ids
        for (const otherPerson of peopleToMerge) {
            const otherPersonDistinctIds: PersonDistinctId[] = (
                await this.db.postgresQuery(
                    'SELECT * FROM posthog_persondistinctid WHERE person_id = $1 AND team_id = $2',
                    [otherPerson, mergeInto.team_id]
                )
            ).rows
            for (const personDistinctId of otherPersonDistinctIds) {
                await this.db.updateDistinctId(personDistinctId, { person_id: mergeInto.id })
            }

            const otherCohortPeople: CohortPeople[] = (
                await this.db.postgresQuery('SELECT * FROM posthog_cohortpeople WHERE person_id = $1', [otherPerson.id])
            ).rows
            for (const cohortPeople of otherCohortPeople) {
                await this.db.postgresQuery('UPDATE posthog_cohortpeople SET person_id = $1 WHERE id = $2', [
                    mergeInto.id,
                    cohortPeople.id,
                ])
            }

            await this.db.deletePerson(otherPerson.id)
        }
    }

    private async captureEE(
        eventUuid: string,
        personUuid: string,
        ip: string,
        siteUrl: string,
        teamId: number,
        event: string,
        distinctId: string,
        properties: Properties,
        timestamp: DateTime,
        sentAt: DateTime | null
    ): Promise<IEvent> {
        event = sanitizeEventName(event)

        const elements: Record<string, any>[] | undefined = properties['$elements']
        let elementsList: Element[] = []
        if (elements && elements.length) {
            delete properties['$elements']
            elementsList = elements.map((el) => ({
                text: el['$el_text']?.slice(0, 400),
                tag_name: el['tag_name'],
                href: el['attr__href']?.slice(0, 2048),
                attr_class: el['attr__class']?.split(' '),
                attr_id: el['attr__id'],
                nth_child: el['nth_child'],
                nth_of_type: el['nth_of_type'],
                attributes: Object.fromEntries(Object.entries(el).filter(([key]) => key.startsWith('attr__'))),
            }))
        }

        const teamQueryResult = await this.db.postgresQuery('SELECT * FROM posthog_team WHERE id = $1', [teamId])
        const team: Team = teamQueryResult.rows[0]

        if (!team.anonymize_ips && !('$ip' in properties)) {
            properties['$ip'] = ip
        }

        this.storeNamesAndProperties(team, event, properties)

        const pdiSelectResult = await this.db.postgresQuery(
            'SELECT COUNT(*) AS pdicount FROM posthog_persondistinctid WHERE team_id = $1 AND distinct_id = $2',
            [teamId, distinctId]
        )
        const pdiCount = parseInt(pdiSelectResult.rows[0].pdicount)

        if (!pdiCount) {
            // Catch race condition where in between getting and creating, another request already created this user
            try {
                const personCreated: Person = await this.db.createPerson(
                    sentAt || DateTime.utc(),
                    {},
                    teamId,
                    null,
                    false,
                    personUuid.toString()
                )
                await this.db.addDistinctId(personCreated, distinctId)
            } catch {}
        }

        return await this.createEvent(eventUuid, event, team, distinctId, properties, timestamp, elementsList)
    }

    private async storeNamesAndProperties(team: Team, event: string, properties: Properties): Promise<void> {
        // In _capture we only prefetch a couple of fields in Team to avoid fetching too much data
        let save = false
        if (!team.ingested_event) {
            // First event for the team captured
            // TODO: capture "first team event ingested"
            team.ingested_event = true
            save = true
        }
        if (team.event_names && !(event in team.event_names)) {
            save = true
            team.event_names.push(event)
            team.event_names_with_usage.push({ event: event, usage_count: null, volume: null })
        }
        for (const [key, value] of Object.entries(properties)) {
            if (team.event_properties && !(key in team.event_properties)) {
                team.event_properties.push(key)
                team.event_properties_with_usage.push({ key: key, usage_count: null, volume: null })
                save = true
            }
            if (
                typeof value === 'number' &&
                team.event_properties_numerical &&
                !(key in team.event_properties_numerical)
            ) {
                team.event_properties_numerical.push(key)
                save = true
            }
        }
        if (save) {
            await this.db.postgresQuery(
                `UPDATE posthog_team SET
                    ingested_event = $1, event_names = $2, event_names_with_usage = $3, event_properties = $4,
                    event_properties_with_usage = $5, event_properties_numerical = $6
                WHERE id = $7`,
                [
                    team.ingested_event,
                    JSON.stringify(team.event_names),
                    JSON.stringify(team.event_names_with_usage),
                    JSON.stringify(team.event_properties),
                    JSON.stringify(team.event_names_with_usage),
                    JSON.stringify(team.event_properties_numerical),
                    team.id,
                ]
            )
        }
    }

    private async createEvent(
        uuid: string,
        event: string,
        team: Team,
        distinctId: string,
        properties?: Properties,
        timestamp?: DateTime | string,
        elements?: Element[]
    ): Promise<IEvent> {
        const timestampString = castTimestampOrNow(timestamp)
        const elementsChain = elements && elements.length ? elementsToString(elements) : ''

        const data: IEvent = {
            uuid,
            event,
            properties: JSON.stringify(properties ?? {}),
            timestamp: timestampString,
            teamId: team.id,
            distinctId,
            elementsChain,
            createdAt: timestampString,
        }

        await this.kafkaProducer.send({
            topic: KAFKA_EVENTS,
            messages: [{ key: uuid, value: EventProto.encodeDelimited(EventProto.create(data)).finish() as Buffer }],
        })

        return data
    }

    private async createSessionRecordingEvent(
        uuid: string,
        team_id: number,
        distinct_id: string,
        session_id: string,
        timestamp: DateTime | string,
        snapshot_data: Record<any, any>
    ): Promise<SessionRecordingEvent> {
        const timestampString = castTimestampOrNow(timestamp)

        const data: SessionRecordingEvent = {
            uuid,
            team_id: team_id,
            distinct_id: distinct_id,
            session_id: session_id,
            snapshot_data: JSON.stringify(snapshot_data),
            timestamp: timestampString,
            created_at: timestampString,
        }

        await this.kafkaProducer.send({
            topic: KAFKA_SESSION_RECORDING_EVENTS,
            messages: [{ key: uuid, value: Buffer.from(JSON.stringify(data)) }],
        })

        return data
    }
}
