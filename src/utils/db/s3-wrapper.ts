import { S3 } from 'aws-sdk'

import { status } from '../status'

export class S3Wrapper {
    s3: S3

    constructor(options: S3.Types.ClientConfiguration) {
        this.s3 = new S3(options)
    }

    async upload(params: S3.Types.PutObjectRequest): Promise<S3.Types.PutObjectOutput> {
        return await this.s3.upload(params).promise()
    }

    async putObject(params: S3.Types.PutObjectRequest): Promise<S3.Types.PutObjectOutput> {
        try {
            status.info('#', 's3.putObject', params)
            const put = this.s3.putObject(params)

            put.on('validate', function (r) {
                status.info('#', '---validate')
            })
            put.on('build', function (r) {
                status.info('#', '---build')
            })
            put.on('sign', function (r) {
                status.info('#', '---sign')
            })
            put.on('send', function (r) {
                status.info('#', '---send')
            })
            put.on('retry', function (r) {
                status.info('#', '---retry')
            })
            put.on('extractError', function (r) {
                status.info('#', '---extractError')
            })
            put.on('extractData', function (r) {
                status.info('#', '---extractData')
            })
            put.on('success', function (r) {
                status.info('#', '---success')
            })
            put.on('error', function (e, r) {
                status.info('#', '---error')
            })
            put.on('complete', function (r) {
                status.info('#', '---complete')
            })
            put.on('httpHeaders', function (s, h, r) {
                status.info('#', '---httpHeaders')
            })
            put.on('httpData', function (c, r) {
                status.info('#', '---httpData')
            })
            put.on('httpUploadProgress', function (p, r) {
                status.info('#', '---httpUploadProgress')
            })
            put.on('httpDownloadProgress', function (p, r) {
                status.info('#', '---httpDownloadProgress')
            })
            put.on('httpError', function (e, r) {
                status.info('#', '---httpError')
            })
            put.on('httpDone', function (r) {
                status.info('#', '---httpDone')
            })

            return await put.promise()
        } finally {
            status.info('#', 's3.putObject DONE')
        }
    }

    async getObject(params: S3.GetObjectRequest): Promise<S3.Types.GetObjectOutput> {
        return await this.s3.getObject(params).promise()
    }

    async deleteObject(params: S3.DeleteObjectRequest): Promise<S3.Types.DeleteObjectOutput> {
        return await this.s3.deleteObject(params).promise()
    }

    async listObjectsV2(params: S3.ListObjectsV2Request): Promise<S3.Types.ListObjectsV2Output> {
        return await this.s3.listObjectsV2(params).promise()
    }
}
