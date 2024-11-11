const express = require("express");
const fs = require("fs");
const cluster = require("cluster");
const os = require("os");

const PORT = 8000;
const numCPUs = 2; //replica set
const app = express();
app.use(express.json());


const rateLimitData = {}; 

const logTaskCompletion = (user_id) => {
    const logMessage = `${user_id} - task completed at - ${new Date().toISOString()}\n`;
    fs.appendFileSync("task_log.txt", logMessage); //logfile
};

// Task function
async function task(user_id) {
    logTaskCompletion(user_id);
    console.log(`${user_id} - task completed at - ${Date.now()}`);
}

// Rate limit middleware
const rateLimiter = async (req, res, next) => {
    const userId = req.body.user_id;
    if (!userId) return res.status(400).send("User ID required");

    if (!rateLimitData[userId]) {
        rateLimitData[userId] = { tasks: [], lastTaskTime: 0, taskCount: 0 };
    }

    const userRateData = rateLimitData[userId];
    const currentTime = Date.now();

   
    if (currentTime - userRateData.lastTaskTime > 60000) {
        userRateData.taskCount = 0;
    }

    
    if (currentTime - userRateData.lastTaskTime >= 1000 && userRateData.taskCount < 20) {
        userRateData.lastTaskTime = currentTime;
        userRateData.taskCount++;
        next();  
    } else {
      
        userRateData.tasks.push({ req, res });
    }
};

// Function for queued tasks
const processQueue = () => {
    for (const userId in rateLimitData) {
        const userRateData = rateLimitData[userId];

        if (userRateData.tasks.length > 0) {
            const currentTime = Date.now();

            if (currentTime - userRateData.lastTaskTime >= 1000 && userRateData.taskCount < 20) {
                const { req, res } = userRateData.tasks.shift();
                userRateData.lastTaskTime = currentTime;
                userRateData.taskCount++;
                task(userId).then(() => res.send("Task processed from queue"));
            }
        }
    }
};

// Set interval to process queue every second
setInterval(processQueue, 1000);

// Route 
app.post("/api/v1/task", rateLimiter, async (req, res) => {
    const userId = req.body.user_id;
    await task(userId);
    res.send("Task processed");
});

//  cluster
if (cluster.isPrimary) {
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
} else {
    app.listen(PORT, () => console.log(`Worker ${process.pid} started on port ${PORT}`));
}
