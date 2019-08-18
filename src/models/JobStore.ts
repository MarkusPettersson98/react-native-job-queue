import { Job } from './Job';

/**
 * maps typescript to native functions
 */
export interface JobStore {
    addJob(job: Job): Promise<void>;
    getJobs(): Promise<Job[]>;
    getNextJob(): Promise<Job>;
    getJobsForWorker(name: string, count: number): Promise<Job[]>;
    updateJob(job: Job): void;
    removeJob(job: Job): void;
    deleteAllJobs(): Promise<void>;
}
