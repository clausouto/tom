const { Client, GatewayIntentBits, Role, Guild, Events, TextChannel, version } = require("discord.js");
const { connectDB, saveMessage, savePivot, getPivot } = require("./db.js");
require("dotenv").config();

const intents = Object.values(GatewayIntentBits);
const client = new Client({
    intents: intents,
    partials: ['MESSAGE', 'CHANNEL', 'REACTION'],
    allowedMentions: {
        parse: ['users', 'roles', 'everyone'],
        repliedUser: true,
    }
});

/**
* @param {TextChannel} channel
*/
const getAllMessagesFromChannel = async (channel) => {    
    // Define the cutoff date: December 1, 2025 at 00:00
    const cutoffDate = new Date('2025-12-01T00:00:00');
    const cutoffTimestamp = cutoffDate.getTime();
    console.log(`Cutoff date set to: ${cutoffDate.toLocaleString()} (${cutoffTimestamp})`);
    
    // Get saved pivot to resume from crash
    let pivot = await getPivot(channel.id);
    if (!pivot) {
        pivot = '1323788890632355840';
        console.log(`No saved pivot found, starting from: ${pivot}`);
    } else {
        console.log(`Resuming from saved pivot: ${pivot}`);
    }
    
    let iteration = 0;
    let totalSaved = 0;
    let reachedCutoff = false;

    while (true) {
        iteration++;
        console.log(`Fetch iteration ${iteration}, pivot: ${pivot}`);
        
        const startTime = Date.now();
        const pack = await channel.messages.fetch({
            limit: 100,
            after: pivot,
        });
        const fetchTime = Date.now() - startTime;

        console.log(`Fetched ${pack.size} messages in iteration ${iteration} (took ${fetchTime}ms)`);
        
        if (pack.size === 0) {
            console.log(`No more messages to fetch. Breaking loop.`);
            break;
        }

        pivot = pack.first().id;
        
        // Save all messages in this batch
        for (const entry of pack.values()) {
            const messageDate = new Date(entry.createdTimestamp);
            const timestamp = messageDate.toLocaleString();
            
            // Check if message has reached or passed the cutoff date (>= Dec 1, 2025 00:00)
            if (entry.createdTimestamp >= cutoffTimestamp) {
                console.log(`⏹ Reached cutoff date at message ${entry.id} (${timestamp})`);
                console.log(`Message timestamp: ${entry.createdTimestamp}, Cutoff: ${cutoffTimestamp}`);
                reachedCutoff = true;
                break;
            }
            
            // Save to MongoDB
            try {
                const result = await saveMessage(entry);
                if (!result.skipped) {
                    totalSaved++;
                    console.log(`✓ Saved message ${entry.id} to MongoDB (Total: ${totalSaved})`);
                }
            } catch (error) {
                console.error(`✗ Failed to save message ${entry.id}:`, error.message);
            }
            
            // Clear reaction cache to prevent memory leaks
            if (entry.reactions && entry.reactions.cache.size > 0) {
                entry.reactions.cache.clear();
            }
        }
        
        // Break the outer loop if we reached the cutoff
        if (reachedCutoff) {
            console.log(`Stopping fetch process - cutoff date reached`);
            break;
        }
        
        // Save pivot after each batch
        try {
            await savePivot(channel.id, pivot);
        } catch (error) {
            console.error(`Failed to save pivot:`, error.message);
        }
        
        // Add delay to respect rate limits
        if (pack.size === 100) {
            console.log(`Waiting 2000ms to respect rate limits...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

    console.log(`Total messages processed: Total saved: ${totalSaved}`);
};

client.once(Events.ClientReady, async () => {
    console.log('Bot is ready and logged in!');
    console.log(`Node version: ${process.version}`);
    console.log(`Discord.js version: ${version}`);
    
    // Connect to MongoDB first
    await connectDB();
        
    const channel = client.channels.cache.get("481896480399949825");
    console.log(`Target channel: ${channel.name} (${channel.id})`);
    
    try {
        await getAllMessagesFromChannel(channel);
    } catch (error) {
        console.error(`Failed to fetch messages:`, error);
    }
    //user.send("Hello! The bot is now online.");
});

client.login(process.env.DISCORD_TOKEN);

// Graceful shutdown handler
process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    try {
        const { closeDB } = require('./db.js');
        await closeDB();
    } catch (error) {
        console.error('Error closing database:', error);
    }
    client.destroy();
    process.exit(0);
});