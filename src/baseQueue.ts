import { IJob, IQueue } from '@rheas/contracts/queue';

export abstract class BaseQueue implements IQueue {
    /**
     * Name of this queue.
     *
     * @var string
     */
    protected _name: string;

    /**
     * The number of jobs to be processed concurrently.
     *
     * @var number
     */
    protected _concurrency: number = 2;

    /**
     * The queue worker process time handler id.
     *
     * @var Timeout
     */
    protected _pollTimer: NodeJS.Timeout | undefined;

    /**
     * The ms interval at which the queue worker has to poll for
     * new job.
     *
     * @var number
     */
    protected _pollInterval: number = 100;

    /**
     * Timeout for a job process. A job process will timout after this value
     * freeing up the worker.
     *
     * @var number
     */
    protected _jobTimeout: number = 10000;

    /**
     * Creates a new Queue with the given name.
     *
     * @param name
     */
    constructor(name: string) {
        this._name = name;
    }

    /**
     * Inserts a new job to the queue.
     *
     * @param job
     */
    public abstract async insert(job: IJob): Promise<boolean>;

    /**
     * Gets the next jobs in the queue. We will fetch n number of jobs as set
     * by the _concurrency property.
     *
     * @returns
     */
    public abstract getNextJobs(): Promise<IJob[]>;

    /**
     * Mark the job as failed on the active queue list. The job will
     * be retried when the reserved time passes the retry time period.
     *
     * @param job
     */
    public abstract async failJob(job: IJob, err?: Error): Promise<any>;

    /**
     * Marks the job as failed forever. This happens when the job has
     * already hit the max retry limit. Remove the job from the active
     * queue list, as we won't retry it again.
     *
     * @param job
     */
    public abstract async failJobForever(job: IJob): Promise<any>;

    /**
     * Mark the job as finished and remove it from the active queue.
     *
     * @param job
     */
    public abstract finishJob(job: IJob): Promise<any>;

    /**
     * Sets the number of jobs to be processed concurrently.
     *
     * @param concurrentJobs
     */
    public setConcurrency(concurrentJobs: number): IQueue {
        this._concurrency = concurrentJobs || this._concurrency;

        return this;
    }

    /**
     * Sets the number of jobs to be processed concurrently.
     *
     * @param inSeconds
     */
    public setTimeout(inSeconds: number): IQueue {
        this._jobTimeout = Math.trunc(inSeconds * 1000 || this._jobTimeout);

        return this;
    }

    /**
     * The queue works on itself during the lifetime of the application. We
     * poll queue every n seconds set by `_pollInterval` property, for the next
     * jobs and process them.
     *
     * External workers can also consume jobs, if they have access to the queue
     * store.
     *
     * @returns
     */
    public async work(): Promise<any> {
        if (this._pollTimer) {
            clearTimeout(this._pollTimer);
        }

        try {
            const jobProcessors = (await this.getNextJobs()).map((job) => this.processJob(job));

            // Wait for all the job processors to finish.
            await Promise.all(jobProcessors);
        } catch (err) {
            // Thrown when there is some error fetching next jobs in
            // queue. Do nothing. We will retry again in the next cycle.
        }

        this._pollTimer = setTimeout(() => this.work(), this._pollInterval);
    }

    /**
     * Processes a job if it has not already exceeded the max retry attempts
     * limit. If the processing causes any exception, we will mark the job as
     * failed. Even though it is marked as failed, it won't be immediately removed
     * until the max attempts are reached.
     *
     * @param job
     */
    public async processJob(job: IJob): Promise<any> {
        return new Promise((resolve, reject) => {
            // Check if the job has hit maxAttempts. If so, mark the
            // job as permenantly failed and immediately resolve this
            // promise.
            if (job.triedMaxAttempts()) {
                return this.failJobForever(job)
                    .catch((err) => err)
                    .finally(resolve);
            }

            // Cancelled jobs should return immediately. Ideally the worker
            // should not be sending in any cancelled jobs to this function.
            // However, we will keep this check in here just in case if we
            // process a job outside the queue worker cycle.
            if (job.isCancelled()) {
                return resolve();
            }

            // Before starting the processing of job, set a job timeout, so that the
            // worker won't be blocked forever processing this job. As soon as the
            // timout hits, this promise will be rejected and the job will be marked
            // as failed even if the process is still running. This causes the job to
            // complete, if it was not freezed, at a later time and not getting marked
            // as finished by the current queue work cycle. If the job finishes before
            // the next cycle, it will be marked completed and won't be processed again.
            // But if the job didn't finish before the next work cycle, it will be
            // processed again causing a single job to be processed multiple times.
            const processTimeout = setTimeout(() => reject('Timed out!'), this._jobTimeout);

            job.process()
                // Mark the process as finished, if the job processed
                // successfully.
                .then(() => this.finishJob(job))
                .then(() => clearTimeout(processTimeout))
                .then(resolve)
                .catch((err) => reject(err));

            // Possibly some error processing the job. Mark the job as failed,
            // log the exception etc.
        }).catch((reason) => this.failJob(job).catch((err) => err));
    }

    /**
     * Call the job events safely.
     *
     * @param event
     */
    protected raiseEvent(event: () => any) {
        try {
            return event();
        } catch (err) {}
    }
}
