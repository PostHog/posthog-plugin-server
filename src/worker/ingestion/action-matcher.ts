import { PluginEvent } from '@posthog/plugin-scaffold'
import { Properties } from '@posthog/plugin-scaffold/src/types'
import escapeStringRegexp from 'escape-string-regexp'

import {
    Action,
    ActionStep,
    ActionStepUrlMatching,
    CohortPropertyFilter,
    Element,
    ElementPropertyFilter,
    EventPropertyFilter,
    PersonPropertyFilter,
    PropertyFilter,
    PropertyFilterWithOperator,
    PropertyOperator,
} from '../../types'
import { DB } from '../../utils/db/db'
import { extractElements } from '../../utils/utils'
import { ActionManager } from './action-manager'

export class ActionMatcher {
    private db: DB
    private actionManager: ActionManager

    // Exposed actionManager methods
    public reloadAllActions: typeof ActionManager.prototype.reloadAllActions
    public reloadAction: typeof ActionManager.prototype.reloadAction
    public dropAction: typeof ActionManager.prototype.dropAction

    constructor(db: DB, actionManager: ActionManager) {
        this.db = db
        this.actionManager = actionManager
        this.reloadAllActions = this.actionManager.reloadAllActions.bind(this.actionManager)
        this.reloadAction = this.actionManager.reloadAction.bind(this.actionManager)
        this.dropAction = this.actionManager.dropAction.bind(this.actionManager)
    }

    /** Get all actions matched to the event. */
    public match(event: PluginEvent): Action[] {
        const teamActions: Action[] = Object.values(this.actionManager.getTeamActions(event.team_id))
        const rawElements: Record<string, any>[] | undefined = event.properties?.['$elements']
        const elements: Element[] = rawElements ? extractElements(rawElements) : []
        const matches: Action[] = teamActions.filter((action) => this.checkAction(event, elements, action))
        return matches
    }

    /**
     * Base level of action matching.
     *
     * Return whether the event is a match for the action.
     * The event is considered a match if any of the action's steps (match groups) is a match.
     */
    public checkAction(event: PluginEvent, elements: Element[], action: Action): boolean {
        return action.steps.some((step) => this.checkStep(event, elements, step))
    }

    /**
     * Sublevel 1 of action matching.
     *
     * Return whether the event is a match for the step (match group).
     * The event is considered a match if no subcheck fails. Many subchecks are usually irrelevant and skipped.
     */
    private checkStep(event: PluginEvent, elements: Element[], step: ActionStep): boolean {
        return (
            this.checkStepElement(elements, step) &&
            this.checkStepUrl(event, step) &&
            this.checkStepEvent(event, step) &&
            this.checkStepFilters(event, elements, step)
            /* && this.checkStepName(event, step) – is ActionStep.name relevant at all??? */
        )
    }

    /**
     * Sublevel 2 of action matching.
     *
     * Return whether the event is a match for the step's "URL" constraint.
     * Step properties: `url_matching`, `url`.
     */
    private checkStepUrl(event: PluginEvent, step: ActionStep): boolean {
        // CHECK CONDITIONS, OTHERWISE SKIPPED
        if (step.url_matching) {
            const stepUrl = step.url || ''
            const eventUrl = event.properties?.$current_url
            if (!eventUrl || typeof eventUrl !== 'string') {
                return false // URL IS UNKNOWN
            }
            let isUrlOk: boolean
            switch (step.url_matching) {
                case ActionStepUrlMatching.Contains:
                    // Simulating SQL LIKE behavior (_ = any single character, % = any zero or more characters)
                    // TODO: reconcile syntax discrepancies between SQL and JS regex
                    const adjustedRegExpString = escapeStringRegexp(stepUrl).replace(/_/g, '.').replace(/%/g, '.*')
                    isUrlOk = new RegExp(`.*${adjustedRegExpString}.*`).test(eventUrl)
                    break
                case ActionStepUrlMatching.Regex:
                    isUrlOk = new RegExp(stepUrl).test(eventUrl)
                    break
                case ActionStepUrlMatching.Exact:
                    isUrlOk = stepUrl === eventUrl
                    break
                default:
                    throw new Error(`Unrecognized ActionStep.url_matching value ${step.url_matching}!`)
            }
            if (!isUrlOk) {
                return false // URL IS A MISMATCH
            }
        }
        return true
    }

    /**
     * Sublevel 2 of action matching.
     *
     * Return whether the event is a match for
     * the step's "Link href equals", "Text equals" and "HTML selector matches" constraints.
     * Step properties: `tag_name`, `text`, `href`, `selector`.
     */
    private checkStepElement(elements: Element[], step: ActionStep): boolean {
        // CHECK CONDITIONS, OTHERWISE SKIPPED
        if (step.href || step.text || (step.href && elements.length)) {
            return elements.some((element) => {
                if (step.href && element.href !== step.href) {
                    return false // ELEMENT HREF IS A MISMATCH
                }
                if (step.tag_name && element.tag_name !== step.tag_name) {
                    return false // ELEMENT TAG NAME IS A MISMATCH
                }
                if (step.text && element.text !== step.text) {
                    return false // ELEMENT TEXT IS A MISATCH
                }
                // TODO: add checking against step.selector
                return true
            }) // AT LEAST ONE ELEMENT MUST BE A SUBMATCH
        }
        return true
    }

    /**
     * Sublevel 2 of action matching.
     *
     * Return whether the event is a match for the step's event name constraint.
     * Step property: `event`.
     */
    private checkStepEvent(event: PluginEvent, step: ActionStep): boolean {
        // CHECK CONDITIONS, OTHERWISE SKIPPED
        if (step.event && event.event !== step.event) {
            return false // EVENT NAME IS A MISMATCH
        }
        return true
    }

    /**
     * Sublevel 2 of action matching.
     *
     * Return whether the event is a match for the step's fiter constraints.
     * Step property: `properties`.
     */
    private checkStepFilters(event: PluginEvent, elements: Element[], step: ActionStep): boolean {
        // CHECK CONDITIONS, OTHERWISE SKIPPED, OTHERWISE SKIPPED
        if (step.properties && step.properties.length) {
            // EVERY FILTER MUST BE A MATCH
            return step.properties.every((filter) => checkEventAgainstFilter(event, elements, filter))
        }
        return true
    }
}

/**
 * Sublevel 3 of action matching.
 */
function checkEventAgainstFilter(event: PluginEvent, elements: Element[], filter: PropertyFilter): boolean {
    switch (filter.type) {
        case 'event':
            return checkEventAgainstEventFilter(event, filter)
        case 'person':
            return checkEventAgainstPersonFilter(event, filter)
        case 'element':
            return checkEventAgainstElementFilter(elements, filter)
        case 'cohort':
            return checkEventAgainstCohortFilter(event, filter)
        default:
            return false
    }
}

const propertyOperatorToRequiredValueType: Partial<Record<PropertyOperator, string>> = {
    [PropertyOperator.IContains]: 'string',
    [PropertyOperator.NotIContains]: 'string',
    [PropertyOperator.Regex]: 'string',
    [PropertyOperator.NotRegex]: 'string',
}

/**
 * Sublevel 5 of action matching.
 */
function checkPropertiesAgainstFilter(
    properties: Properties | null | undefined,
    filter: PropertyFilterWithOperator
): boolean {
    if (!properties) {
        return false // MISMATCH DUE TO LACK OF PROPERTIES THAT COULD FULFILL CONDITION
    }

    const possibleValues = Array.isArray(filter.value) ? filter.value : [filter.value]
    const foundValue = properties[filter.key]
    let foundValueLowerCase: string // only calculated if needed for a case-insensitive operator

    const requiredValueType = propertyOperatorToRequiredValueType[filter.operator]
    if (requiredValueType && typeof foundValue !== requiredValueType) {
        return false // MISMATCH DUE TO VALUE TYPE INCOMPATIBLE WITH OPERATOR SUPPORT
    }

    let test: (possibleValue: any) => boolean
    switch (filter.operator) {
        case PropertyOperator.Exact:
            test = (possibleValue) => possibleValue === foundValue
            break
        case PropertyOperator.IsNot:
            test = (possibleValue) => possibleValue !== foundValue
            break
        case PropertyOperator.IContains:
            foundValueLowerCase = foundValue.toLowerCase()
            test = (possibleValue) =>
                typeof possibleValue === 'string' && foundValueLowerCase.includes(possibleValue.toLowerCase())
            break
        case PropertyOperator.NotIContains:
            foundValueLowerCase = foundValue.toLowerCase()
            test = (possibleValue) =>
                typeof possibleValue === 'string' && !foundValueLowerCase.includes(possibleValue.toLowerCase())
            break
        case PropertyOperator.Regex:
            test = (possibleValue) => typeof possibleValue === 'string' && new RegExp(possibleValue).test(foundValue)
            break
        case PropertyOperator.NotRegex:
            test = (possibleValue) => typeof possibleValue === 'string' && !new RegExp(possibleValue).test(foundValue)
            break
        case PropertyOperator.GreaterThan:
            test = (possibleValue) => foundValue > possibleValue
            break
        case PropertyOperator.LessThan:
            test = (possibleValue) => foundValue < possibleValue
            break
        case PropertyOperator.IsSet:
            test = () => foundValue !== undefined
            break
        case PropertyOperator.IsNotSet:
            test = () => foundValue === undefined
            break
        default:
            throw new Error(`Operator ${filter.operator} is unknown and can't be used for event property filtering!`)
    }

    return possibleValues.some(test) // ANY OF POSSIBLE VALUES MUST BE A MATCH AGAINST THE FOUND VALUE
}

/**
 * Sublevel 4 of action matching.
 */
function checkEventAgainstEventFilter(event: PluginEvent, filter: EventPropertyFilter): boolean {
    return checkPropertiesAgainstFilter(event.properties, filter)
}

/**
 * Sublevel 4 of action matching.
 */
function checkEventAgainstPersonFilter(event: PluginEvent, filter: PersonPropertyFilter): boolean {
    return checkPropertiesAgainstFilter(event.properties, filter) // TODO: get person here for use instead of event
}

/**
 * Sublevel 4 of action matching.
 */
function checkEventAgainstElementFilter(elements: Element[], filter: ElementPropertyFilter): boolean {
    // TODO: make sure this makes sense this way!
    return elements.some((element) => checkPropertiesAgainstFilter(element, filter))
}

/**
 * Sublevel 4 of action matching.
 */
function checkEventAgainstCohortFilter(event: PluginEvent, filter: CohortPropertyFilter): boolean {
    // TODO: check against dynamic cohort (CohortPeople)
    // TODO: check against static cohort for ClickHouse – realistically can be done later as this is almost unused
    // and not present in original query_db_by_action
    return false
}