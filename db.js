const { MongoClient } = require("mongodb");
require("dotenv").config();

let db = null;
const uri = process.env.MONGODB_URI;
const mongoClient = new MongoClient(uri);

async function connectDB(dbname = 'tom') {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(dbname);
    console.log('Connected to MongoDB');
    
    // Create indexes for better query performance
    const messagesCollection = db.collection('messages');
    await messagesCollection.createIndex({ messageId: 1 }, { unique: true });
    await messagesCollection.createIndex({ "author": 1 });
    await messagesCollection.createIndex({ "channel": 1 });
    await messagesCollection.createIndex({ "timestamps.created": -1 });
    console.log('Indexes created');
  }
  return db;
}

async function saveMessage(message) {
  try {
    const db = await connectDB();
    const messagesCollection = db.collection('messages');
    
    // Check if message already exists
    const existingMessage = await messagesCollection.findOne({ messageId: message.id });
    if (existingMessage) {
      console.log(`Message ${message.id} already exists, skipping...`);
      return { skipped: true };
    }

    // Process reactions
    const reactions = [];
    if (message.reactions && message.reactions.cache.size > 0) {
      for (const reaction of message.reactions.cache.values()) {
        const emoji = reaction.emoji.id 
          ? { id: reaction.emoji.id, name: reaction.emoji.name, animated: reaction.emoji.animated }
          : { name: reaction.emoji.name };
        
        // Fetch users who reacted (if needed for detailed tracking)
        const users = [];
        try {
          const reactionUsers = await reaction.users.fetch();
          reactionUsers.forEach(user => {
            users.push({
              id: user.id,
              tag: user.tag,
              username: user.username,
              bot: user.bot
            });
          });
        } catch (error) {
          console.log(`Could not fetch reaction users: ${error.message}`);
        }

        reactions.push({
          emoji: emoji,
          count: reaction.count,
          me: reaction.me,
          users: users
        });
      }
      // Clear reaction cache after processing
      message.reactions.cache.clear();
    }

    // Build the document
    const messageDoc = {
      messageId: message.id,
      content: message.content,
      author: message.author.id,
      channel: message.channel.id,
      guild: message.guild.id,
      timestamps: {
        created: new Date(message.createdTimestamp),
        edited: message.editedTimestamp ? new Date(message.editedTimestamp) : null,
        savedAt: new Date()
      },
      reactions: reactions,
      type: message.type,
      flags: message.flags.bitfield,
      pinned: message.pinned,
    };

    // Upsert (insert or update if exists)
    const result = await messagesCollection.updateOne(
      { messageId: message.id },
      { $set: messageDoc },
      { upsert: true }
    );

    return result;
  } catch (error) {
    console.error(`Failed to save message ${message.id}:`, error);
    throw error;
  }
}

async function savePivot(channelId, pivotId) {
  try {
    const db = await connectDB();
    const pivotCollection = db.collection('fetch_pivot');
    
    await pivotCollection.updateOne(
      { channelId },
      { 
        $set: {
          channelId,
          lastPivotId: pivotId,
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );
    
    console.log(`Saved pivot: ${pivotId}`);
  } catch (error) {
    console.error(`Failed to save pivot:`, error);
    throw error;
  }
}

async function getPivot(channelId) {
  try {
    const db = await connectDB();
    const pivotCollection = db.collection('fetch_pivot');
    
    const pivot = await pivotCollection.findOne({ channelId });
    if (pivot) {
      console.log(`Retrieved pivot: ${pivot.lastPivotId}`);
      return pivot.lastPivotId;
    }
    return null;
  } catch (error) {
    console.error(`Failed to get pivot:`, error);
    throw error;
  }
}

async function closeDB() {
  if (mongoClient) {
    await mongoClient.close();
    db = null;
    console.log('MongoDB connection closed');
  }
}

module.exports = { connectDB, saveMessage, savePivot, getPivot, closeDB };