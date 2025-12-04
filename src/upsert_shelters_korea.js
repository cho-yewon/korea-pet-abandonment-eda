// src/upsert_shelters_korea.js
import dotenv from "dotenv";
dotenv.config();

import { MongoClient } from "mongodb";

const { MONGODB_URI, NAAS_KEY } = process.env;
if (!MONGODB_URI) throw new Error("MONGODB_URI 없음");
if (!NAAS_KEY) throw new Error("NAAS_KEY 없음");

// 동물보호센터 정보 조회서비스 v2
// Requested Link: http://apis.data.go.kr/1543061/animalShelterSrvc_v2/shelterInfo_v2 :contentReference[oaicite:9]{index=9}
const BASE = "http://apis.data.go.kr/1543061/animalShelterSrvc_v2";

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
const arrify = (x) => (Array.isArray(x) ? x : x ? [x] : []);

// 공통 호출
async function callShelterInfo({ pageNo, numOfRows = 1000, maxRetries = 3 }) {
  const url = new URL(`${BASE}/shelterInfo_v2`);

  url.searchParams.set("serviceKey", NAAS_KEY);
  url.searchParams.set("_type", "json");
  url.searchParams.set("numOfRows", String(numOfRows));
  url.searchParams.set("pageNo", String(pageNo));

  let lastErr = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url);
      const text = await res.text();

      if (!res.ok) {
        console.error(
          `❗ shelterInfo_v2 HTTP 오류 status=${res.status}, body:\n`,
          text.slice(0, 500)
        );
        throw new Error(`HTTP ${res.status}`);
      }

      let json;
      try {
        json = JSON.parse(text);
      } catch (e) {
        console.error(
          "❗ shelterInfo_v2 JSON 파싱 실패, 원본 응답:\n",
          text.slice(0, 500)
        );
        throw new Error("JSON 파싱 실패: " + e.message);
      }

      const header = json?.response?.header;
      const body = json?.response?.body;

      if (!header || !body) {
        console.error(
          "❗ shelterInfo_v2 응답 구조가 예상과 다름, 전체 JSON:\n",
          JSON.stringify(json, null, 2).slice(0, 2000)
        );
        throw new Error("응답에 response.header/body 없음");
      }

      const resultCode = header.resultCode;
      const resultMsg = header.resultMsg;

      if (resultCode !== "00") {
        console.error(
          `❗ shelterInfo_v2 API 오류 resultCode=${resultCode}, resultMsg=${resultMsg}`
        );
        throw new Error(`API 오류 ${resultCode} ${resultMsg}`);
      }

      const totalCount = Number(body.totalCount ?? 0);
      const items = arrify(body.items?.item ?? []);

      return { items, totalCount };
    } catch (e) {
      lastErr = e;
      console.warn(
        `⚠️ shelterInfo_v2 호출 오류 (pageNo=${pageNo}, 시도 ${attempt}/${maxRetries}): ${e.message}`
      );
      await sleep(1000 * attempt);
    }
  }

  throw new Error(
    `🚫 shelterInfo_v2(pageNo=${pageNo}) 재시도 ${maxRetries}회 모두 실패: ${lastErr?.message}`
  );
}

// 보호소 정규화
function normalizeShelter(row) {
  const careRegNo =
    row.careRegNo || row.care_reg_no || row.CENTER_ID || null;
  const careNm = row.careNm || row.care_nm || row.CENTER_NM || null;
  const orgNm = row.orgNm || row.org_nm || row.ORG_NM || null;
  const divisionNm = row.divisionNm || row.division_nm || null;
  const careAddr =
    row.careAddr ||
    row.care_addr ||
    row.ADDR ||
    row.roadAddr ||
    row.jibunAddr ||
    null;
  const careTel =
    row.careTel || row.care_tel || row.TELNO || row.phoneNumber || null;

  const latRaw = row.lat || row.latitude || row.LAT;
  const lngRaw = row.lng || row.longitude || row.LNG;

  const lat = latRaw != null ? Number(latRaw) : null;
  const lng = lngRaw != null ? Number(lngRaw) : null;

  const openDate =
    row.dsignationDate ||
    row.openDate ||
    row.OPEN_DT ||
    row.dsignation_date ||
    null;
  const closeDate =
    row.closeDate ||
    row.close_dt ||
    row.CLOSE_DT ||
    row.closureDate ||
    null;

  return {
    uid: careRegNo || `${careNm || ""}_${careAddr || ""}`,
    careRegNo: careRegNo || null,
    careNm,
    orgNm,
    divisionNm,
    careAddr,
    careTel,
    lat: Number.isFinite(lat) ? lat : null,
    lng: Number.isFinite(lng) ? lng : null,
    openDate,
    closeDate,
    raw: row,
  };
}

async function main() {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  const db = client.db("animals");
  const col = db.collection("shelters");

  console.log("✅ MongoDB 연결 — 전국 보호소 수집 시작");

  const numOfRows = 1000;
  let pageNo = 1;
  let total = 0;
  let totalCountGlobal = null;

  while (true) {
    console.log(`\n📄 shelterInfo_v2 pageNo=${pageNo} 요청 중...`);

    const { items, totalCount } = await callShelterInfo({
      pageNo,
      numOfRows,
    });

    if (totalCountGlobal === null) {
      totalCountGlobal = totalCount;
      console.log(`🔢 API totalCount = ${totalCountGlobal}`);
    }

    if (!items.length) {
      console.log(
        `⛔ pageNo=${pageNo} 에서 더 이상 데이터 없음 → 수집 종료`
      );
      break;
    }

    const ops = items.map((r) => {
      const doc = normalizeShelter(r);
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
    total += wrote;

    console.log(
      `✅ pageNo=${pageNo} → upsert=${wrote} (누적=${total}), items=${items.length}`
    );

    const maxPage =
      totalCountGlobal != null
        ? Math.ceil(totalCountGlobal / numOfRows)
        : null;

    if (items.length < numOfRows) {
      console.log(
        "✅ 마지막 페이지 도달 (items < numOfRows) → 수집 종료"
      );
      break;
    }
    if (maxPage !== null && pageNo >= maxPage) {
      console.log(
        `✅ pageNo=${pageNo}가 maxPage=${maxPage} 이상 → 수집 종료`
      );
      break;
    }

    pageNo++;
    await sleep(300);
  }

  console.log(`\n🏁 전국 보호소 수집 완료 — 총 upsert=${total}`);
  await client.close();
}

main().catch((err) => {
  console.error("❌ shelters 전국 수집 실패:", err);
  process.exit(1);
});
