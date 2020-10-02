import { Str } from '@rheas/support/str';
import { JsonObject } from '@rheas/contracts';
import { IQueableData } from '@rheas/contracts/queue';
import { ILaterTime } from '@rheas/contracts/notifications';
import { IJob, IJobMetaData } from '@rheas/contracts/queue';
import { queue as queueManager } from '@rheas/support/helpers';

export abstract class BaseJob<T extends JsonObject> implements IJob<T> {
    /**
     * The unique job id.
     *
     * @var string
     */
    protected _id: string | undefined;

    /**
     * The queue in which this job has to be processed.
     *
     * @var string
     */
    protected _queue: string = '';

    /**
     * Maximum number of attempts to be made before marking this
     * job as failed.
     *
     * @var number
     */
    protected _maxAttempts = 5;

    /**
     * Number of times the job was attempted by the workers.
     *
     * @var number
     */
    protected _attempts = 0;

    /**
     * The wait period between two retries.
     *
     * @var number
     */
    protected _retryWaitInMillis = 0;

    /**
     * The UNIX epoch time at which the job was last processed.
     *
     * @var number
     */
    protected _lockedAt: number = 0;

    /**
     * The UNIX epoch time at which the job has to be processed.
     *
     * @var number
     */
    protected _availableAt: number = Date.now();

    /**
     * Cancel flag of this job.
     *
     * @var boolean
     */
    protected _cancelled: boolean = false;

    /**
     * Data this job needs to process.
     *
     * @var JsonObject
     */
    protected _data: T;

    /**
     * Creates a new job with the data it needs to process the job. The data
     * must be in a plain JSON format that can be stored as a string and parsed
     * back to process the job.
     *
     * @param data
     */
    constructor(data: T) {
        this._data = data;
    }

    /**
     * Contains the tasks to be performed by this job.
     *
     * @returns
     */
    public abstract process(): Promise<any>;

    /**
     * Returns the meta data of this job. The data should include the job
     * `fileName` which is the full path to the file and `export` property,
     * which is the name of the job class in the export object. With these
     * properties we can recreate the job class at a later time.
     *
     * @returns
     */
    public abstract metaData(): IJobMetaData;

    /**
     * Executed when the job processes successfully.
     *
     * Avoid blocking code in here.
     */
    public async onSuccess(): Promise<any> {}

    /**
     * Executed whenever the job processing fails.
     *
     * Avoid blocking code in here.
     */
    public async onFailure(): Promise<any> {}

    /**
     * Executed when the job is marked as failed forever ie, when the job
     * is no longer retried.
     *
     * Avoid blocking code in here.
     */
    public async onPermanentFailure(): Promise<any> {}

    /**
     * Cancels the job. Marks the job as cancelled and requests the queue to remove
     * this job from it.
     *
     * @returns
     */
    public async cancel(): Promise<IJob<T>> {
        this._cancelled = true;

        queueManager(this.queue()).cancelJob(this);

        return this;
    }

    /**
     * Returns the unique job id, if it set or we will create a new one.
     *
     * @returns
     */
    public async id(): Promise<string> {
        if (!this._id) {
            this._id = await Str.random(32);
        }
        return this._id;
    }

    /**
     * Sets the name of the queue in which this job has to be send to.
     *
     * @param queue
     */
    public onQueue(queue: string): IJob<T> {
        this._queue = queue;

        return this;
    }

    /**
     * Sets the maximum number of process attempts.
     *
     * @param attempts
     */
    public maxAttempts(attempts: number): IJob<T> {
        this._maxAttempts = attempts;

        return this;
    }

    /**
     * Sets the retry wait period.
     *
     * @param seconds Number of seconds after which a failed job has to be retried.
     */
    public retryAfter(seconds: number): IJob<T> {
        this._retryWaitInMillis = Math.trunc(seconds * 1000);

        return this;
    }
    /**
     * Sets a delay on the job schedule.
     *
     * @param later
     */
    public later(later: ILaterTime): IJob<T> {
        this._availableAt = later.atTime();

        return this;
    }

    /**
     * Returns the name of the queue in which the job is present.
     *
     * @returns
     */
    public queue(): string {
        return this._queue;
    }

    /**
     * Returns the number of times worker attempted to process this job.
     *
     * @returns
     */
    public attempts(): number {
        return this._attempts;
    }

    /**
     * Returns the time at which last attempt was made.
     *
     * @returns
     */
    public lockTime(): number {
        return this._lockedAt;
    }

    /**
     * Returns the time in UNIX ms at which the job is ready to be
     * processed.
     *
     * @returns
     */
    public availableAt(): number {
        return this._availableAt;
    }

    /**
     * Returns true if the job has hit the maxAttempts limit.
     *
     * @returns
     */
    public triedMaxAttempts(): boolean {
        return this.attempts() >= this._maxAttempts;
    }

    /**
     * Returns true if the job has not surpassed the retry period.
     *
     * @returns
     */
    public isStillLocked(): boolean {
        return Date.now() < this._retryWaitInMillis + this._lockedAt;
    }

    /**
     * Returns true if the job is ready for processing ie, the delay time
     * has surpassed.
     *
     * @returns
     */
    public isAvailable(): boolean {
        return Date.now() >= this._availableAt;
    }

    /**
     * Returns true if the job is cancelled. Cancelled jobs won't be processed.
     *
     * @returns
     */
    public isCancelled(): boolean {
        return this._cancelled;
    }

    /**
     * Returns the data that has to be saved in the queue store, so that the job
     * can be retreived later from the store and can be parsed to process it.
     *
     * @returns
     */
    public queableData(): IQueableData<T> {
        return {
            data: this._data,
            __meta: this.metaData(),
        };
    }
}
