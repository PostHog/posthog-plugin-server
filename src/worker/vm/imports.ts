import * as bigquery from '@google-cloud/bigquery'
import * as contrib from '@posthog/plugin-contrib'
import * as scaffold from '@posthog/plugin-scaffold'
import * as AWS from 'aws-sdk'
import crypto from 'crypto'
import * as genericPool from 'generic-pool'
import fetch from 'node-fetch'
import * as pg from 'pg'
import snowflake from 'snowflake-sdk'
import * as url from 'url'
import * as zlib from 'zlib'

import { writeToFile } from './extensions/test-utils'

export const imports = {
    crypto: crypto,
    url: url,
    zlib: zlib,
    'generic-pool': genericPool,
    'node-fetch': fetch,
    'snowflake-sdk': snowflake,
    '@google-cloud/bigquery': bigquery,
    '@posthog/plugin-scaffold': scaffold,
    '@posthog/plugin-contrib': contrib,
    'aws-sdk': AWS,
    pg: pg,
    ...(process.env.NODE_ENV === 'test'
        ? {
              'test-utils/write-to-file': writeToFile,
          }
        : {}),
}
