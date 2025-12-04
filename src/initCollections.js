// src/initCollections.js
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
dotenv.config();

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    const db = client.db("animals");

    console.log("🐾 MongoDB 연결 성공 — 컬렉션 및 인덱스 생성 시작");

    // ---------- 1. abandonments (유기·입양 동물)
    const ab = db.collection("abandonments");
    await ab.createIndex({ uid: 1 }, { unique: true });
    await ab.createIndex({ eventDate: 1 });
    await ab.createIndex({ "location.geo": "2dsphere" });
    await ab.createIndex({ "location.sido": 1, "location.sigungu": 1, species: 1, eventDate: -1 });

    // ---------- 2. shelters (보호소)
    const sh = db.collection("shelters");
    await sh.createIndex({ uid: 1 }, { unique: true });
    await sh.createIndex({ "location.geo": "2dsphere" });

    // ---------- 3. registrations (등록현황)
    const rg = db.collection("registrations");
    await rg.createIndex({ uid: 1 }, { unique: true });
    await rg.createIndex({ year: 1, month: 1, sigungu: 1, species: 1 });

    console.log("✅ 컬렉션 & 인덱스 생성 완료");
  } catch (err) {
    console.error("❌ 오류 발생:", err.message);
  } finally {
    await client.close();
  }
}

run();
