// src/upsert_abandonments_korea.js

import dotenv from "dotenv";
dotenv.config();

import { MongoClient } from "mongodb";

const { MONGODB_URI, NAAS_KEY } = process.env;
if (!MONGODB_URI) throw new Error("MONGODB_URI 없음");
if (!NAAS_KEY) throw new Error("NAAS_KEY 없음");

// v2 베이스 URL
const BASE = "https://apis.data.go.kr/1543061/abandonmentPublicService_v2";

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
const arrify = (x) => (Array.isArray(x) ? x : x ? [x] : []);

// ======================= 날짜 유틸 =========================

// 'YYYYMMDD' → JS Date
function parseYmd(str) {
  if (!str || str.length !== 8) {
    throw new Error(`잘못된 날짜 형식: ${str}`);
  }
  const y = Number(str.slice(0, 4));
  const m = Number(str.slice(4, 6)) - 1; // 0~11
  const d = Number(str.slice(6, 8));
  return new Date(y, m, d);
}

// Date → 'YYYYMMDD'
function formatYmd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

// 해당 월의 마지막 날짜
function endOfMonth(date) {
  const y = date.getFullYear();
  const m = date.getMonth();
  return new Date(y, m + 1, 0); // 다음 달 0일 → 이번 달 마지막 날
}

// start~end 사이의 월별 구간 리스트 생성
// 예: 2023-01-10 ~ 2023-03-05
//  → [ [2023-01-10 ~ 2023-01-31],
//      [2023-02-01 ~ 2023-02-28],
//      [2023-03-01 ~ 2023-03-05] ]
function buildMonthlyRanges(startStr, endStr) {
  const startDate = parseYmd(startStr);
  const endDate = parseYmd(endStr);

  if (startDate > endDate) {
    throw new Error(`AB_START(${startStr})가 AB_END(${endStr})보다 큼`);
  }

  const ranges = [];

  // cur는 해당 월의 1일
  let cur = new Date(startDate.getFullYear(), startDate.getMonth(), 1);

  while (cur <= endDate) {
    const monthStart = new Date(cur); // 이 월의 1일
    const monthEnd = endOfMonth(cur);

    // 실제 구간: 전체 범위와 겹치는 부분만 사용
    const realStart = monthStart < startDate ? startDate : monthStart;
    const realEnd = monthEnd > endDate ? endDate : monthEnd;

    ranges.push({
      start: formatYmd(realStart),
      end: formatYmd(realEnd),
      label: `${realStart.getFullYear()}-${String(
        realStart.getMonth() + 1
      ).padStart(2, "0")}`,
    });

    // 다음 달 1일로 이동
    cur = new Date(cur.getFullYear(), cur.getMonth() + 1, 1);
  }

  return ranges;
}

// ======================================================
// 공통 API 호출 함수 (v2)
// - 5xx 에러 시 재시도
// - 재시도 후에도 실패하면 throw (스킵 X, 전체 종료)
// ======================================================
async function call(endpoint, params = {}, maxRetries = 5) {
  const url = new URL(`${BASE}/${endpoint}`);

  url.searchParams.set("serviceKey", NAAS_KEY);
  url.searchParams.set("_type", "json");

  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, String(v));
    }
  }

  let lastErr = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url);

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        // 5xx → 재시도 대상
        if (res.status >= 500 && res.status < 600) {
          lastErr = new Error(
            `status=${res.status} body=${text.slice(0, 200)}`
          );
          console.warn(
            `⚠️ ${endpoint} (upr_cd=${url.searchParams.get(
              "upr_cd"
            )}, org_cd=${url.searchParams.get(
              "org_cd"
            )}, bgnde=${url.searchParams.get(
              "bgnde"
            )}, endde=${url.searchParams.get(
              "endde"
            )}) 시도 ${attempt}/${maxRetries} 실패 → 재시도 대기`
          );
          await sleep(1000 * attempt); // 1초, 2초, 3초, ...
          continue;
        }

        // 4xx 같은 건 바로 실패 처리
        throw new Error(
          `status=${res.status} body=${text.slice(0, 200)}`
        );
      }

      const json = await res.json().catch((e) => {
        throw new Error("JSON 파싱 실패: " + e.message);
      });
      const items = json?.response?.body?.items?.item;
      return arrify(items);
    } catch (e) {
      lastErr = e;
      console.warn(
        `⚠️ ${endpoint} (upr_cd=${params.upr_cd}, org_cd=${params.org_cd}, bgnde=${params.bgnde}, endde=${params.endde}) 호출 중 오류 (시도 ${attempt}/${maxRetries}): ${e.message}`
      );
      await sleep(1000 * attempt);
    }
  }

  // 여기까지 왔으면 재시도 다 실패 → 전체 종료를 위해 throw
  throw new Error(
    `🚫 ${endpoint} (upr_cd=${params.upr_cd}, org_cd=${params.org_cd}, bgnde=${params.bgnde}, endde=${params.endde}) 재시도 ${maxRetries}회 모두 실패`
  );
}

// ---------------------- 시도 / 시군구 ----------------------
async function fetchSidos() {
  const items = await call("sido_v2", { numOfRows: 100, pageNo: 1 }, 5);
  return items || [];
}

async function fetchSigungu(upr_cd) {
  const items = await call(
    "sigungu_v2",
    { upr_cd, numOfRows: 500, pageNo: 1 },
    5
  );
  return items || [];
}

// ---------------------- 유기동물 ----------------------
async function fetchAbandonments(params) {
  const items = await call("abandonmentPublic_v2", params, 5);
  return items || [];
}

// ---------------------- 정규화 ----------------------
function normalizeAbandonment(r) {
  const happenDt = String(r.happenDt ?? "");
  const year = happenDt.slice(0, 4);
  const month = happenDt.slice(4, 6);

  return {
    uid: r.desertionNo,
    desertionNo: r.desertionNo,
    noticeNo: r.noticeNo ?? null,
    happenDt,
    year: Number(year) || null,
    month: Number(month) || null,

    uprCd: r.uprCd ?? null,
    orgCd: r.orgCd ?? null,
    sido: r.orgNmSido ?? null,
    sigungu: r.orgNm ?? null,

    kindCd: r.kindCd ?? null,
    kindNm: r.kindNm ?? null,
    colorCd: r.colorCd ?? null,
    ageRaw: r.age ?? null,
    weightRaw: r.weight ?? null,
    sexCd: r.sexCd ?? null,
    neuterYn: r.neuterYn ?? null,

    careNm: r.careNm ?? null,
    careAddr: r.careAddr ?? null,
    careTel: r.careTel ?? null,

    processState: r.processState ?? null,
    specialMark: r.specialMark ?? null,

    popfile: r.popfile ?? null,
    filename: r.filename ?? null,

    raw: r,
  };
}

// ======================================================
//                      MAIN
// ======================================================
async function main() {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  const db = client.db("animals");
  const col = db.collection("abandonments");

  console.log("✅ MongoDB 연결 — 전국 유기동물 수집 시작");

  // 2. 고정 기간 설정 (2020-01-01 ~ 2024-12-31)
  const AB_START = "20240101";
  const AB_END = "20241231";

  console.log(`📅 수집 기간: ${AB_START} ~ ${AB_END}`);

  // 3. 월 단위 구간 생성
  const monthRanges = buildMonthlyRanges(AB_START, AB_END);
  console.log(
    `📌 월 단위 구간 수: ${monthRanges.length}개월 → ${monthRanges
      .map((r) => r.label)
      .join(", ")}`
  );

  // 4. 시도 목록 가져오기
  const sidos = await fetchSidos();
  console.log(`📌 시도 개수: ${sidos.length}`);

  if (!sidos.length) {
    throw new Error("시도 목록을 하나도 가져오지 못함");
  }

  let total = 0;

  // 5. 월 단위로 전체 루프
  for (const range of monthRanges) {
    console.log(
      `\n==================== [${range.label}] ${range.start} ~ ${range.end} 처리 시작 ====================`
    );

    for (const s of sidos) {
      const upr_cd = s.orgCd ?? s.uprCd;
      const sidoNm = s.orgdownNm ?? s.orgNm ?? "(시도이름없음)";
      if (!upr_cd) continue;

      console.log(
        `\n===== ${sidoNm} (${upr_cd}) 처리 시작 [${range.label}] =====`
      );

      const sigungus = await fetchSigungu(upr_cd);
      console.log(`  ▶ 시군구 수: ${sigungus.length}`);

      // 🔵 세종특별자치시 같은 "시군구 없음" 시도 처리
      if (!sigungus.length) {
        console.log(
          `  ℹ️ ${sidoNm}는 시군구가 없으므로 upr_cd 단위로 직접 조회 [${range.label}]`
        );

        let pageNo = 1;
        while (true) {
          const rows = await fetchAbandonments({
            bgnde: range.start,
            endde: range.end,
            upr_cd,
            // org_cd 없이 호출
            pageNo,
            numOfRows: 1000,
          });

          if (!rows.length) {
            console.log(
              `  ⛔ ${sidoNm} [${range.label}] page=${pageNo} 데이터 없음 → 이 시도/월 종료`
            );
            break;
          }

          const ops = rows.map((r) => {
            const doc = normalizeAbandonment(r);
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
            `  ✅ ${sidoNm} [${range.label}] (no sigungu) page=${pageNo} → upsert=${wrote} (누적=${total})`
          );

          if (rows.length < 1000) {
            break;
          }

          pageNo++;
          await sleep(200);
        }

        // 시군구 루프는 건너뛰고, 다음 시도로
        continue;
      }

      // 🟩 일반 시도 (시군구 있는 경우)
      for (const g of sigungus) {
        const org_cd = g.orgCd;
        const sigunguNm = g.orgdownNm ?? g.orgNm ?? "(시군구이름없음)";
        if (!org_cd) continue;

        let pageNo = 1;

        while (true) {
          const rows = await fetchAbandonments({
            bgnde: range.start,
            endde: range.end,
            upr_cd,
            org_cd,
            pageNo,
            numOfRows: 1000,
          });

          if (!rows.length) {
            console.log(
              `  ⛔ ${sigunguNm} [${range.label}] page=${pageNo} 데이터 없음 → 다음 시군구/월`
            );
            break;
          }

          const ops = rows.map((r) => {
            const doc = normalizeAbandonment(r);
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
            `  ✅ ${sigunguNm} [${range.label}] page=${pageNo} → upsert=${wrote} (누적=${total})`
          );

          if (rows.length < 1000) {
            // 마지막 페이지
            break;
          }

          pageNo++;
          await sleep(200); // 서버 부담 줄이기
        }
      }
    }

    console.log(
      `\n✅ [${range.label}] ${range.start} ~ ${range.end} 처리 완료 (현재까지 누적 upsert=${total})`
    );
  }

  console.log(`\n🏁 전국 유기동물 수집 완료 — 총 upsert=${total}`);
  await client.close();
}

main().catch((err) => {
  console.error("❌ abandonments 전국 수집 실패:", err);
  process.exit(1);
});
