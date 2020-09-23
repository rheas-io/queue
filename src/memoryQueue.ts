import { BaseQueue } from './baseQueue';
import { IJob } from '@rheas/contracts/queue';

export class MemoryQueue extends BaseQueue {
    /**
     * Stores the jobs in the memory.
     *
     * @var array
     */
    protected _jobs: IJob[] = [];

    /**
     * Inserts a new job to the queue.
     *
     * @param job
     */
    public insert(job: IJob): Promise<boolean> {
        this._jobs.push(job);

        return Promise.resolve(true);
    }

    /**
     * Returns jobs from the memory queue. We will filter out the jobs ready for
     * processing and sort them by dispatch time. The jobs that has to be sent early
     * are popped out. Only a max of n jobs are returned where n is the number of concurrent
     * jobs allowed by this queue.
     *
     * @returns
     */
    public getNextJobs(): Promise<IJob[]> {
        const poppedJobs: IJob[] = this._jobs
            .filter(this.jobReadyForProcessing)
            .sort((job_1, job_2) => job_1.availableAt() - job_2.availableAt());

        if (this._concurrency && poppedJobs.length > this._concurrency) {
            poppedJobs.length = this._concurrency;
        }

        return Promise.resolve(poppedJobs);
    }

    /**
     * Returns true if the given job is ready for processing.
     *
     * @param job
     */
    protected jobReadyForProcessing(job: IJob): boolean {
        return !job.isStillLocked() && job.isAvailable() && !job.isCancelled();
    }

    /**
     * Marks the job as failed.
     *
     * @param job
     * @param err
     */
    public failJob(job: IJob, err?: Error): Promise<any> {
        this.raiseEvent(job.onFailure);

        return Promise.resolve(true);
    }

    /**
     * Removes a job permanently from the queue. Also log the failure message,
     * so that it's recorded somewhere.
     *
     * @param job
     */
    public failJobForever(job: IJob): Promise<any> {
        this.removeJobFromQueue(job);

        this.raiseEvent(job.onPermanentFailure);

        //TODO Log the failure message.

        return Promise.resolve();
    }

    /**
     * Remove the job from the active queue list. Raise events if
     *
     * @param job
     */
    public finishJob(job: IJob): Promise<any> {
        this.removeJobFromQueue(job);

        this.raiseEvent(job.onSuccess);

        return Promise.resolve(true);
    }

    /**
     * Removes the job from the queue.
     *
     * @param job
     */
    protected removeJobFromQueue(job: IJob) {
        const index = this._jobs.indexOf(job);

        if (index != 1) {
            this._jobs.splice(index, 1);
        }
    }
}
