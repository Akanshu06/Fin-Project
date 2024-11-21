const express = require('express');
const cluster = require('cluster');
const redis = require('redis');
const fs = require('fs');
const { promisify } = require('util');

// Redis client setup
const redisClient = redis.createClient();
const getAsync = promisify(redisClient.get).bind(redisClient);
const setAsync = promisify(redisClient.set).bind(redisClient);
const lpushAsync = promisify(redisClient.lPush).bind(redisClient);
const brpopAsync = promisify(redisClient.brPop).bind(redisClient);

// Task function to log task completion
async function task(user_id) {
    const logMessage = `${user_id}-task completed at-${Date.now()}`;
    console.log(logMessage);

    // Append log to log file
    fs.appendFile('task-log.txt', logMessage + '\n', (err) => {
        if (err) throw err;
    });
}

// Rate Limiting function
async function checkRateLimit(user_id) {
    const userKey = `user:${user_id}`;
    const now = Date.now();

    const lastExecutedSecond = await getAsync(`${userKey}:last-second`);
    const executedInLastMinute = await getAsync(`${userKey}:minute-tasks`);

    // 1 task per second check
    if (lastExecutedSecond && now - parseInt(lastExecutedSecond) < 1000) {
        return false;
    }

    // 20 tasks per minute check
    if (executedInLastMinute && parseInt(executedInLastMinute) >= 20) {
        return false;
    }

    // Update last execution time for second-based rate limiting
    await setAsync(`${userKey}:last-second`, now);
    // Increment task count for minute-based rate limiting
    await redisClient.incrby(`${userKey}:minute-tasks`, 1);
    await redisClient.expire(`${userKey}:minute-tasks`, 60); // expire after 1 minute

    return true;
}

// Queue processing logic
async function processTaskQueue() {
    while (true) {
        const [user_id] = await brpopAsync('task-queue', 0); // Blocks until a task is available

        // Check rate limit before processing
        const canExecute = await checkRateLimit(user_id);
        if (canExecute) {
            await task(user_id); // Process the task
        } else {
            // Re-enqueue the task if rate limit exceeded
            console.log(`Rate limit exceeded for user ${user_id}, re-enqueuing task.`);
            await lpushAsync('task-queue', user_id);
        }
    }
}

// Cluster setup for multiple replicas
if (cluster.isMaster) {
    // Fork workers
    for (let i = 0; i < 2; i++) {
        cluster.fork();
    }

    // Handle worker death
    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });
} else {
    // Worker process
    const app = express();
    app.use(express.json());

    // Route to handle task requests
    app.post('/task', async (req, res) => {
        const { user_id } = req.body;

        if (!user_id) {
            return res.status(400).json({ error: 'user_id is required' });
        }

        // Add task to queue
        await lpushAsync('task-queue', user_id);
        res.status(202).json({ message: 'Task is being processed' });
    });

    app.listen(3000, () => {
        console.log(`Worker ${process.pid} started`);
    });

    // Start processing the task queue
    processTaskQueue();
}
