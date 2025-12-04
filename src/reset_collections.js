// src/reset_collections.js
import dotenv from 'dotenv';
dotenv.config();

import { MongoClient } from 'mongodb';

const { MONGODB_URI } = process.env;
if (!MONGODB_URI) throw new Error('MONGODB_URI 없음');

async function main() {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db('animals');

  console.log('🧹 animals DB 초기화 시작');

  for (const name of ['abandonments', 'shelters', 'registrations']) {
    const col = db.collection(name);
    const res = await col.deleteMany({});
    console.log(`✅ ${name} 삭제 완료: deletedCount=${res.deletedCount}`);
  }

  await client.close();
  console.log('🏁 초기화 끝');
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
