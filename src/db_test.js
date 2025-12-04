import { MongoClient } from 'mongodb';
import 'dotenv/config';

const uri = process.env.MONGODB_URI;

async function run() {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db('animals');
    const col = db.collection('test');

    await col.insertOne({ message: 'Hello MongoDB!', createdAt: new Date() });
    const docs = await col.find().toArray();
    console.log('✅ 연결 성공! sample data:', docs);

    await client.close();
  } catch (err) {
    console.error('❌ 연결 실패:', err);
  }
}
run();
