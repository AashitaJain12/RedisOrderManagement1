const express = require('express');
const bodyParser = require('body-parser');
const redis = require('ioredis');

const app = express();
const PORT = process.env.PORT || 3000;

// Redis client
const redisClient = new redis();

// Middleware to parse JSON bodies
app.use(bodyParser.json());

console.log("Connecting to Redis...");

redisClient.on("connect", () => { 
    console.log("Connected to Redis!"); 
});

// Function to add client information to Redis
async function addClientInfo(req, res) {
    const key = req.body.ClientId;
    const hashname = `${req.body.TenantId}_${req.body.OSMId}_${key}`;
    
    try {
        const exists = await redisClient.hexists(hashname, key);
        if (exists) {
            res.status(400).json({ error: 'Client already exists' });
        } else {
            await redisClient.hset(hashname, key, JSON.stringify(req.body));
            console.log('client added successfully to Redis');
            res.json({ message: 'client added successfully' });
        }
    } catch (error) {
        console.error('Error adding client information to Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to update client information in Redis
async function updateClientInfo(req, res) {
    const key = req.body.ClientId;
    const hashname = `${req.body.TenantId}_${req.body.OSMId}_${key}`;
    
    try {
        const exists = await redisClient.hexists(hashname, key);
        if (exists) {
            await redisClient.hset(hashname, key, JSON.stringify(req.body));
            console.log('clientInfo updated successfully');
            res.json({ message: 'clientInfo updated successfully' });
        } else {
            res.status(400).json({ error: 'Client does not exist' });
        }
    } catch (error) {
        console.error('Error updating client information to Redis:', error); 
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to retrieve client information from Redis for a given ClientID
async function getAllClients(req, res) {
     
    try {
        const hashnames = await redisClient.keys('*_*');
        console.log({hashnames})
        const clientInfo = [];
for (let hashname of hashnames) {
    
    const hashInfo = await redisClient.hgetall(hashname);
    clientInfo.push(hashInfo);
}
        if (!clientInfo.length) {
            return res.send('Client does not exist');
        }
        console.log('Client retrieved successfully');
        res.json(JSON.parse(JSON.stringify(clientInfo)));
    } catch (error) {
        console.error('Error getting client information from Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to retrieve all client information from Redis for a given TenantId and OSMId
async function getClient(req, res) {

    const { TenantId, OSMId } = req.body;
    if (!TenantId || !OSMId) {
        return res.status(400).json({ error: 'TenantId and OSMId are required from ' });
    }
    try {
        const key = req.body.ClientId;
        
        const hashname = `${TenantId}_${OSMId}_${key}`;
        console.log(hashname);
        const clients = await redisClient.hgetall(hashname);
       console.log({clients})
        if (!clients || Object.keys(clients).length === 0) {
            return res.send('No clients found');
        }
        console.log('Clients retrieved successfully');
        res.json(clients);
    } catch (error) {
        console.error('Error getting clients information from Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to remove client information from Redis
async function removeclientInfo(req, res) {
    const key = req.body.ClientID;
    const hashname = `${req.body.TenantId}_${req.body.OSMId}_${key}`;
    
    try {
        const exists = await redisClient.hexists(hashname, key);
        if (exists) {
            await redisClient.hdel(hashname, key);
            console.log('req.body removed successfully from Redis');
            res.json({ message: 'req.body removed successfully' });
        } else {
            res.status(400).json({ error: 'Client does not exist' });
        }
    } catch (error) {
        console.error('Error removing client information from Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// POST endpoint for adding client info
app.post('/clientInfo/add', (req, res) => {
    const { MsgType, OperationType } = req.body;
    if (MsgType !== 1121 || OperationType !== 100) {
        return res.status(400).send('Invalid MsgType or OperationType');
    }
    addClientInfo(req , res);
});

// POST endpoint for updating client info
app.post('/clientInfo/update', (req, res) => {
    const { MsgType, OperationType } = req.body;
    if (MsgType !== 1121 || OperationType !== 101) {
        return res.status(400).send('Invalid MsgType or OperationType');
    }
    updateClientInfo(req, res);
});

// POST endpoint for removing client info
app.post('/clientInfo/remove', (req, res) => {
    const { MsgType, OperationType } = req.body;
    if (MsgType !== 1121 || OperationType !== 102) {
        return res.status(400).send('Invalid MsgType or OperationType');
    }
    removeclientInfo(req,res);
});

// POST endpoint for retrieving client info by ClientID
app.post('/clientInfo/get', (req, res) => {
    const { MsgType, OperationType } = req.body;
    if (MsgType !== 1121 || OperationType !== 103) {
        return res.status(400).send('Invalid MsgType or OperationType');
    }
    getClient(req, res);
});

// POST endpoint for retrieving all client info for a given TenantId and OSMId
app.post('/clientInfo/getAll', (req, res) => {
    const { MsgType, OperationType } = req.body;
    console.log(req.body);
    if (MsgType !== 1121 || OperationType !== 104) {
        return res.status(400).send('Invalid MsgType or OperationType');
    }
    getAllClients(req, res);
});

function getOrderHash(orderInfo) {
    return `${orderInfo.TenantId}_${orderInfo.OSMId}_${orderInfo.ClientID}_${orderInfo.Token}`;
}

// Function to add order information to Redis
async function addOrderInfo(req, res) {
    try {
        const hashname = getOrderHash(req.body);
        const exists = await redisClient.hexists(hashname, req.body.OrderID);
        if (exists) {
            return res.send({ "msg":"Order already exist"});
        }
        await redisClient.hset(hashname, req.body.OrderID, JSON.stringify(req.body));
        const data = await redisClient.hgetall(hashname);
        res.json({ message: 'OrderInfo added successfully',data});
    }
     catch (error) {
        console.error('Error adding order information to Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to update order information to Redis
async function updateOrderInfo(req, res) {
    try {
        const hashname = getOrderHash(req.body);
        const exists = await redisClient.hexists(hashname, req.body.OrderID);
        if (!exists) {
            return res.send('Order does not exist');
        }
        await redisClient.hset(hashname, req.body.OrderID, JSON.stringify(req.body));
        console.log('OrderInfo updated successfully');
        const data = await redisClient.hgetall(hashname);
        res.json({ message: 'OrderInfo updated successfully',data });
    } catch (error) {
        console.error('Error updating order information to Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to remove order information from Redis
async function removeOrderInfo(req, res) {
    try {
        const hashname = getOrderHash(req.body);
        const exists = await redisClient.hexists(hashname, req.body.OrderID);
        if (!exists) {
            return res.send('Order does not exist');
        }
        await redisClient.hdel(hashname, req.body.OrderID);
        console.log('OrderInfo removed successfully from Redis');
        res.json({ message: 'OrderInfo removed successfully' });
    } catch (error) {
        console.error('Error removing order information from Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// Function to retrieve order information from Redis
async function getOrderInfo(req, res) {
    try {
        const hashname = getOrderHash(req.body);
        const orderInfo = await redisClient.hget(hashname, req.body.OrderID);
        if (!orderInfo) {
            return res.send('Order does not exist');
        }
        console.log('OrderInfo retrieved successfully');
        res.json(JSON.parse(orderInfo));
    } catch (error) {
        console.error('Error getting order information from Redis:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
}

// POST endpoint for OrderInfo
app.post('/orderInfo', (req, res) => {
    const { OperationType } = req.body;

    // Validating Message Type
    if (req.body.MsgType !== 1120) {
        return res.send('Message type wrong');
    }

    if (OperationType === 100) {
        addOrderInfo(req, res);
    } else if (OperationType === 101) {
        updateOrderInfo(req, res);
    } else if (OperationType === 102) {
        removeOrderInfo(req, res);
    } else if (OperationType === 103) {
        getOrderInfo(req, res);
    } else {
        res.status(400).json({ error: 'Invalid OperationType' });
    }
});
// Start server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
