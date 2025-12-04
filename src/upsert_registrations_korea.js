// src/upsert_registrations_korea.js
import dotenv from "dotenv";
dotenv.config();

import { MongoClient } from "mongodb";

const { MONGODB_URI, MAFRA_KEY } = process.env;
if (!MONGODB_URI) throw new Error("MONGODB_URI 없음");
if (!MAFRA_KEY) throw new Error("MAFRA_KEY 없음");

// 동물등록 현황 OpenAPI
// http://211.237.50.150:7080/openapi/{API_KEY}/{TYPE}/Grid_20210806000000000612_1/{START}/{END}?CTPV=경기도 :contentReference[oaicite:5]{index=5}
const BASE = "http://211.237.50.150:7080/openapi";
const GRID = "Grid_20210806000000000612_1";

// 전국 시도 목록 (CTPV 값)
const CTPV_LIST = [
  "서울특별시",
  "부산광역시",
  "대구광역시",
  "인천광역시",
  "광주광역시",
  "대전광역시",
  "울산광역시",
  "세종특별자치시",
  "경기도",
  "강원도",
  "충청북도",
  "충청남도",
  "전라북도",
  "전라남도",
  "경상북도",
  "경상남도",
  "제주특별자치도",
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const arrify = (x) => (Array.isArray(x) ? x : x ? [x] : []);

// 숫자 파싱
function toNumberSafe(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/,/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isNaN(n) ? null : n;
}

// 한 페이지 호출
async function fetchPage({ ctpv, startIndex, endIndex, maxRetries = 3 }) {
  // 형식: /{API_KEY}/{TYPE}/{GRID}/{START}/{END}
  const url = new URL(
    `${BASE}/${MAFRA_KEY}/json/${GRID}/${startIndex}/${endIndex}`
  );
  if (ctpv) url.searchParams.set("CTPV", ctpv);

  let lastErr = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url);

      const text = await res.text();
      if (!res.ok) {
        // 서버 에러: 그대로 보여주기
        console.error("❗ 동물등록 API 비정상 응답 (raw):\n", text.slice(0, 500));
        throw new Error(`HTTP ${res.status}`);
      }

      let json;
      try {
        json = JSON.parse(text);
      } catch (e) {
        console.error("❗ JSON 파싱 실패, 원본 응답:\n", text.slice(0, 500));
        throw new Error("JSON 파싱 실패: " + e.message);
      }

      const grid = json[GRID];
      if (!grid) {
        console.error(
          `❗ 응답에 ${GRID} 필드가 없습니다. 전체 JSON:\n`,
          JSON.stringify(json, null, 2).slice(0, 2000)
        );
        throw new Error(`응답에 ${GRID} 필드가 없습니다.`);
      }

      // totalCnt, result, row 구조는 명세와 동일 :contentReference[oaicite:6]{index=6}
      const totalCntRaw = grid.totalCnt ?? grid.TOTALCNT ?? null;
      const totalCnt = toNumberSafe(totalCntRaw) ?? 0;

      const result = grid.result || grid.RESULT || {};
      const code = result.code || result.CODE || null;
      const message = result.message || result.MESSAGE || null;

      if (code && code !== "INFO-000") {
        console.error(
          `❗ 동물등록 API 오류 code=${code}, message=${message}`
        );
        throw new Error(`API 오류 code=${code} message=${message}`);
      }

      const rows = arrify(grid.row || grid.ROW || []);
      return { rows, totalCnt };
    } catch (e) {
      lastErr = e;
      console.warn(
        `⚠️ registrations 호출 중 오류 (CTPV=${ctpv}, start=${startIndex}, end=${endIndex}, 시도 ${attempt}/${maxRetries}): ${e.message}`
      );
      await sleep(1000 * attempt);
    }
  }

  throw new Error(
    `🚫 registrations (CTPV=${ctpv}, start=${startIndex}, end=${endIndex}) 재시도 ${maxRetries}회 모두 실패: ${lastErr?.message}`
  );
}

// 행 정규화
function normalizeRow(row) {
  const ctpv = row.CTPV ?? row.ctpv ?? null; // 시도
  const sgg = row.SGG ?? row.sgg ?? null; // 시군구
  const brdt = row.BRDT ?? row.brdt ?? null; // 생년
  const rfidSe = row.RFID_SE ?? row.rfid_se ?? null; // RFID 구분
  const kind = row.LVSTCK_KND ?? row.lvstck_knd ?? null; // 축종
  const spcs = row.SPCS ?? row.spcs ?? null; // 품종
  const cnt = toNumberSafe(row.CNT ?? row.cnt ?? 0) ?? 0;

  const birthYear = brdt ? Number(String(brdt).slice(0, 4)) : null;

  const uid = [
    ctpv || "NA",
    sgg || "NA",
    brdt || "NA",
    rfidSe || "NA",
    kind || "NA",
    spcs || "NA",
  ].join("|");

  return {
    uid,
    sido: ctpv,
    sigungu: sgg,
    brdt: brdt ?? null,
    birthYear,
    rfidType: rfidSe,
    kind,
    species: spcs,
    count: cnt,
    raw: row,
  };
}

async function main() {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  const db = client.db("animals");
  const col = db.collection("registrations");

  console.log("✅ MongoDB 연결 — 전국 동물등록 현황 수집 시작");

  const pageSize = 1000;
  let totalGlobal = 0;

  for (const ctpv of CTPV_LIST) {
    console.log(`\n==================== [${ctpv}] 수집 시작 ====================`);

    let start = 1;
    let firstTotalCnt = null;
    let localTotal = 0;

    while (true) {
      const end = start + pageSize - 1;
      console.log(
        `📄 CTPV=${ctpv} start=${start} end=${end} 페이지 요청 중...`
      );

      const { rows, totalCnt } = await fetchPage({
        ctpv,
        startIndex: start,
        endIndex: end,
      });

      if (firstTotalCnt === null) firstTotalCnt = totalCnt;

      if (!rows.length) {
        console.log(
          `⛔ CTPV=${ctpv} start=${start} end=${end} 데이터 없음. totalCnt=${firstTotalCnt}`
        );
        break;
      }

      const ops = rows.map((row) => {
        const doc = normalizeRow(row);
        return {
          updateOne: {
            filter: { uid: doc.uid },
            update: { $set: doc },
            upsert: true,
          },
        };
      });

      const result = await col.bulkWrite(ops, { ordered: false });
      const wrote =
        (result.upsertedCount ?? 0) + (result.modifiedCount ?? 0);

      totalGlobal += wrote;
      localTotal += wrote;

      console.log(
        `✅ CTPV=${ctpv} start=${start} end=${end} → upsert=${wrote} (시도 누적=${localTotal}, 전체 누적=${totalGlobal}, totalCnt=${firstTotalCnt})`
      );

      if (rows.length < pageSize) {
        console.log(
          `✅ CTPV=${ctpv} 마지막 페이지 도달 (rows < pageSize) → 이 시도 종료`
        );
        break;
      }
      if (firstTotalCnt !== null && end >= firstTotalCnt) {
        console.log(
          `✅ CTPV=${ctpv} end=${end}가 totalCnt=${firstTotalCnt} 이상 → 이 시도 종료`
        );
        break;
      }

      start += pageSize;
      await sleep(300);
    }

    console.log(
      `🔚 [${ctpv}] 수집 완료 — 시도별 upsert=${localTotal}, totalCnt(추정)=${firstTotalCnt}`
    );
  }

  console.log(
    `\n🏁 전국 registrations 완료. total upserted/modified=${totalGlobal}`
  );
  await client.close();
}

main().catch((e) => {
  console.error("❌ registrations 전국 수집 실패:", e);
  process.exit(1);
});
