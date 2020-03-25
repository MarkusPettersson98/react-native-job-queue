import { Job, RawJob } from './models/Job';

export interface WorkerOptions<P extends object> {
    onStart?: (job: Job<P>) => void;
    onSuccess?: (job: Job<P>) => void;
    onFailure?: (job: Job<P>, error: Error) => void;
    onCompletion?: (job: Job<P>) => void;
    concurrency?: number;
    retries?: number;
}
/**
 * @typeparam P specifies the Type of the Job-Payload.
 */
export class Worker<P extends object> {
    public readonly name: string;
    public readonly concurrency: number;
    public readonly retries: number;

    private executionCount: number;
    private executer: (payload: P) => Promise<any>;

    private onStart: (job: Job<P>) => void;
    private onSuccess: (job: Job<P>) => void;
    private onFailure: (job: Job<P>, error: Error) => void;
    private onCompletion: (job: Job<P>) => void;

    /**
     *
     * @typeparam P specifies the type of the job-payload.
     * @param name of worker
     * @param executer function to run jobs
     * @param options to configure worker
     */
    constructor(name: string, executer: (payload: P) => Promise<any>, options: WorkerOptions<P> = {}) {
        const {
            onStart = (job: Job<P>) => {},
            onSuccess = (job: Job<P>) => {},
            onFailure = (job: Job<P>, error: Error) => {},
            onCompletion = (job: Job<P>) => {},
            concurrency = 5,
            retries = 0
        } = options;

        this.name = name;
        this.concurrency = concurrency;
        this.retries = retries;

        this.executionCount = 0;
        this.executer = executer;

        this.onStart = onStart;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
        this.onCompletion = onCompletion;
    }

    /**
     * @returns true if worker runs max concurrent amout of jobs
     */
    get isBusy() {
        return this.executionCount === this.concurrency;
    }
    /**
     * @returns amount of available Executers for current worker
     */
    get availableExecuters() {
        return this.concurrency - this.executionCount;
    }
    /**
     * This method should not be invoked manually and is used by the queue to execute jobs
     * @param job to be executed
     */
    async execute(rawJob: RawJob) {
        const { timeout } = rawJob;
        const payload: P = JSON.parse(rawJob.payload);
        const job = { ...rawJob, ...{ payload } };
        this.executionCount++;
        try {
            this.onStart(job);
            if (this.retries > 0) {
                await this.executeWithRetry(job, this.retries, timeout, 0);
            } else if (timeout > 0) {
                await this.executeWithTimeout(job, timeout);
            } else {
                await this.executer(payload);
            }
            this.onSuccess(job);
        } catch (error) {
            this.onFailure(job, error);
            throw error;
        } finally {
            this.executionCount--;
            this.onCompletion(job);
        }
    }
    private async executeWithTimeout(job: Job<P>, timeout: number) {
        const timeoutPromise = new Promise((resolve, reject) => {
            setTimeout(() => {
                reject(new Error(`Job ${job.id} timed out`));
            }, timeout);
        });
        await Promise.race([timeoutPromise, this.executer(job.payload)]);
    }

    private wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

    /**
     * Try to execute a job. If that fails, retry after some time period
     *
     * @param {Job} job The job to execute with this worker
     * @param retries Times to retry the job
     * @param timeout Time between retries
     */
    private async executeWithRetry(job: Job<P>, retries: number, timeout: number, retryCount: number) {
        const tmp = new Promise((resolve, reject) => {
            return this.executer(job.payload)
                .then(resolve)
                .catch((reason) => {
                    if (retryCount >= retries) {
                        reject(
                            new Error(`Job ${job.id} was rejected after ${retryCount} retries. Stack trace: ${reason}`)
                        );
                    }
                    return this.wait(timeout).then(async () => {
                        const tmp = this.executeWithRetry(job, retries, timeout, retryCount + 1)
                            .then((result) => result)
                            .catch((error) => error);
                        return tmp;
                    });
                });
        });
        await tmp;
    }
}
