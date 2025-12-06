const { Client, GatewayIntentBits, Role, Guild, Events, TextChannel } = require("discord.js");
const { connectDB, saveMessage, savePivot, getPivot, getMessageCount } = require("./db.js");
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
    const messages = [];
    
    // Define the cutoff date: December 1, 2025 at 00:00
    const cutoffDate = new Date('2025-12-01T00:00:00');
    console.log(`[DEBUG] Cutoff date set to: ${cutoffDate.toLocaleString()}`);
    
    // Get saved pivot to resume from crash
    let pivot = await getPivot(channel.id);
    if (!pivot) {
        pivot = '1323788890632355840';
        console.log(`[DEBUG] No saved pivot found, starting from: ${pivot}`);
    } else {
        console.log(`[DEBUG] Resuming from saved pivot: ${pivot}`);
    }
    
    let iteration = 0;
    let totalSaved = 0;
    let reachedCutoff = false;

    while (true) {
        iteration++;
        console.log(`[DEBUG] Fetch iteration ${iteration}, pivot: ${pivot}`);
        
        const startTime = Date.now();
        const pack = await channel.messages.fetch({
            limit: 100,
            after: pivot,
        });
        const fetchTime = Date.now() - startTime;

        console.log(`[DEBUG] Fetched ${pack.size} messages in iteration ${iteration} (took ${fetchTime}ms)`);
        
        if (pack.size === 0) {
            console.log(`[DEBUG] No more messages to fetch. Breaking loop.`);
            break;
        }

        pivot = pack.first().id;
        
        // Save all messages in this batch
        for (const entry of pack.values()) {
            const messageDate = new Date(entry.createdTimestamp);
            const timestamp = messageDate.toLocaleString();
            
            // Check if message is at or before the cutoff date
            if (messageDate <= cutoffDate) {
                console.log(`[DEBUG] ⏹ Reached cutoff date at message ${entry.id} (${timestamp})`);
                reachedCutoff = true;
                break;
            }
            
            console.log(`[DEBUG] Message: [${entry.author.tag}] [${timestamp}] ${entry.content.substring(0, 50)}${entry.content.length > 50 ? '...' : ''}`);
            messages.push(entry);
            
            // Save to MongoDB
            try {
                const result = await saveMessage(entry);
                if (!result.skipped) {
                    totalSaved++;
                    console.log(`[DEBUG] ✓ Saved message ${entry.id} to MongoDB (Total: ${totalSaved})`);
                }
            } catch (error) {
                console.error(`[ERROR] ✗ Failed to save message ${entry.id}:`, error.message);
            }
        }
        
        // Break the outer loop if we reached the cutoff
        if (reachedCutoff) {
            console.log(`[DEBUG] Stopping fetch process - cutoff date reached`);
            break;
        }
        
        // Save pivot after each batch
        try {
            await savePivot(channel.id, pivot);
        } catch (error) {
            console.error(`[ERROR] Failed to save pivot:`, error.message);
        }
        
        // Add delay to respect rate limits
        if (pack.size === 100) {
            console.log(`[DEBUG] Waiting 5000ms to respect rate limits...`);
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }

    console.log(`[DEBUG] Total messages collected: ${messages.length}, Total saved: ${totalSaved}`);
    return messages;
};

client.once(Events.ClientReady, async () => {
    console.log('[DEBUG] Bot is ready and logged in!');
    console.log(`[DEBUG] Bot username: ${client.user.tag}`);
    
    // Connect to MongoDB first
    await connectDB();
    
    const user = client.guilds.cache.get("481492678442221569").members.cache.get("198040241544757248");
    console.log(`[DEBUG] Target user: ${user.user.tag} (${user.id})`);
    
    const channel = client.channels.cache.get("481896480399949825");
    console.log(`[DEBUG] Target channel: ${channel.name} (${channel.id})`);
    
    // Get existing message count
    const existingCount = await getMessageCount(channel.id);

    try {
        const messages = await getAllMessagesFromChannel(channel);
    } catch (error) {
        console.error(`[ERROR] Failed to fetch messages:`, error);
    }
    //user.send("Hello! The bot is now online.");
});

client.login(process.env.DISCORD_TOKEN);