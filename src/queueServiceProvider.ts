import { QueueManager } from './queueManager';
import { ServiceProvider } from '@rheas/services';
import { InstanceHandler } from '@rheas/contracts/container';

export class QueueServiceProvider extends ServiceProvider {
    /**
     * Returns the queue manager which caches different queues. Queue manager
     * is registered on the app lifecycle.
     *
     * @returns
     */
    public serviceResolver(): InstanceHandler {
        return (app) => new QueueManager();
    }
}
