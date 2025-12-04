# clean_animals.py
import pandas as pd
import numpy as np
import os
from pymongo import MongoClient
from datetime import datetime


MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "animals"

AB_COLLECTION = "abandonments"
REG_COLLECTION = "registrations"
SH_COLLECTION = "shelters"


def get_mongo_db(url: str = MONGO_URL, db_name: str = DB_NAME):
    client = MongoClient(url)
    return client[db_name]


def load_data(db):
    print("MongoDB에서 데이터 로드 중...")
    ab = pd.DataFrame(list(db[AB_COLLECTION].find()))
    reg = pd.DataFrame(list(db[REG_COLLECTION].find()))
    sh = pd.DataFrame(list(db[SH_COLLECTION].find()))
    print(f" - abandonments: {ab.shape}")
    print(f" - registrations: {reg.shape}")
    print(f" - shelters: {sh.shape}")
    return ab, reg, sh


# ---------- 유기·입양 데이터 정제 ----------

def clean_abandonments(ab: pd.DataFrame) -> pd.DataFrame:
    ab = ab.copy()
    print("유기동물(abandonments) 데이터 정제 중...")

    # 1) 날짜 컬럼들
    ab["happenDt"] = pd.to_datetime(ab.get("happenDt"), format="%Y%m%d", errors="coerce")

    # raw 안에 들어있는 날짜들
    def get_raw(x, key):
        if isinstance(x, dict):
            return x.get(key)
        return None

    ab["noticeSdt"] = pd.to_datetime(
        ab["raw"].apply(lambda x: get_raw(x, "noticeSdt")),
        format="%Y%m%d",
        errors="coerce",
    )
    ab["noticeEdt"] = pd.to_datetime(
        ab["raw"].apply(lambda x: get_raw(x, "noticeEdt")),
        format="%Y%m%d",
        errors="coerce",
    )
    ab["updateDt"] = pd.to_datetime(
        ab["raw"].apply(lambda x: get_raw(x, "updTm")),
        errors="coerce",
    )

    # 2) 종 / 품종 분리 (kindFullNm: "[개] 믹스견")
    kind_full = ab["raw"].apply(lambda x: get_raw(x, "kindFullNm"))
    ab["species"] = kind_full.str.extract(r"\[(.*?)\]").iloc[:, 0]
    ab["breed"] = kind_full.str.replace(r"\[.*?\]\s*", "", regex=True)

    # 3) 성별 / 중성화
    ab["sex"] = ab.get("sexCd").map({"M": "Male", "F": "Female", "Q": "Unknown"})
    ab["neuter"] = ab.get("neuterYn").map({"Y": 1, "N": 0, "U": np.nan})

    # 4) 몸무게 숫자만 추출 (raw.weight: "6.8(Kg)")
    weight_raw = ab["raw"].apply(lambda x: get_raw(x, "weight"))
    ab["weight"] = (
        weight_raw
        .astype(str)
        .str.replace(r"\(.*?\)", "", regex=True)
        .replace({"": np.nan})
    )
    ab["weight"] = pd.to_numeric(ab["weight"], errors="coerce")

    # 5) 나이/출생연도 (ageRaw: "2016(년생)")
    if "ageRaw" in ab.columns:
        birth_year = (
            ab["ageRaw"]
            .astype(str)
            .str.extract(r"(\d{4})")[0]
        )
        ab["birthYear"] = pd.to_numeric(birth_year, errors="coerce")
    else:
        ab["birthYear"] = np.nan

    current_year = datetime.now().year
    ab["age"] = current_year - ab["birthYear"]

    # 6) 지역 정보 (sigungu: "서울특별시 강남구")
    def split_sido_sigungu(val):
        if isinstance(val, str):
            parts = val.split()
            if len(parts) >= 2:
                return parts[0], parts[1]
            elif len(parts) == 1:
                return parts[0], None
        return None, None

    tmp = ab.get("sigungu").apply(lambda v: pd.Series(split_sido_sigungu(v), index=["_sido_tmp", "_sigungu_tmp"]))
    ab["_sido_tmp"] = tmp["_sido_tmp"]
    ab["_sigungu_tmp"] = tmp["_sigungu_tmp"]

    # 기존에 sido 컬럼이 비어 있으면 tmp 값으로 채우기
    if "sido" not in ab.columns:
        ab["sido"] = ab["_sido_tmp"]
    else:
        ab["sido"] = ab["sido"].fillna(ab["_sido_tmp"])

    # sigungu는 두 번째 단어만 사용
    ab["sigungu"] = ab["_sigungu_tmp"].where(ab["_sigungu_tmp"].notna(), ab.get("sigungu"))

    ab.drop(columns=["_sido_tmp", "_sigungu_tmp"], inplace=True)

    # 7) 계절 / 요일 / 월
    ab["month"] = ab["happenDt"].dt.month
    ab["weekday"] = ab["happenDt"].dt.day_name()

    def get_season(m):
        if pd.isna(m):
            return np.nan
        m = int(m)
        if m in [3, 4, 5]:
            return "Spring"
        if m in [6, 7, 8]:
            return "Summer"
        if m in [9, 10, 11]:
            return "Fall"
        return "Winter"

    ab["season"] = ab["month"].apply(get_season)

    print("유기동물 정제 완료:", ab.shape)
    return ab


# ---------- 등록(등록현황) 데이터 정제 ----------

def clean_registrations(reg: pd.DataFrame) -> pd.DataFrame:
    reg = reg.copy()
    print("동물등록(registrations) 데이터 정제 중...")

    def get_raw(x, key):
        if isinstance(x, dict):
            return x.get(key)
        return None

    # 시도/시군구
    if "sido" not in reg.columns:
        reg["sido"] = reg["raw"].apply(lambda x: get_raw(x, "CTPV"))
    if "sigungu" not in reg.columns:
        reg["sigungu"] = reg["raw"].apply(lambda x: get_raw(x, "SGG"))

    # 출생연도 (birthYear / BRDT)
    if "birthYear" in reg.columns:
        reg["birthYear"] = pd.to_numeric(reg["birthYear"], errors="coerce")
    else:
        reg["birthYear"] = pd.to_numeric(
            reg["raw"].apply(lambda x: get_raw(x, "BRDT")),
            errors="coerce",
        )

    # RFID 타입
    if "rfidType" not in reg.columns:
        reg["rfidType"] = reg["raw"].apply(lambda x: get_raw(x, "RFID_SE"))

    # 동물종/품종
    if "kind" not in reg.columns:
        reg["kind"] = reg["raw"].apply(lambda x: get_raw(x, "LVSTCK_KND"))
    if "species" not in reg.columns:
        reg["species"] = reg["raw"].apply(lambda x: get_raw(x, "SPCS"))

    # 등록 수량
    if "count" not in reg.columns:
        reg["count"] = pd.to_numeric(
            reg["raw"].apply(lambda x: get_raw(x, "CNT")),
            errors="coerce",
        )
    else:
        reg["count"] = pd.to_numeric(reg["count"], errors="coerce")

    print("등록현황 정제 완료:", reg.shape)
    return reg


# ---------- 보호소 데이터 정제 ----------

def clean_shelters(sh: pd.DataFrame) -> pd.DataFrame:
    sh = sh.copy()
    print("보호소(shelters) 데이터 정제 중...")

    # 시도/시군구: careAddr 기준
    sh["careAddr"] = sh.get("careAddr").astype(str)

    def split_addr(val):
        if isinstance(val, str):
            parts = val.split()
            if len(parts) >= 2:
                return parts[0], parts[1]
            elif len(parts) == 1:
                return parts[0], None
        return None, None

    addr_split = sh["careAddr"].apply(lambda v: pd.Series(split_addr(v), index=["sido", "sigungu"]))
    sh["sido"] = addr_split["sido"]
    sh["sigungu"] = addr_split["sigungu"]

    # 위도/경도: lat/lng가 없으면 raw에서 추출
    def get_raw(x, key):
        if isinstance(x, dict):
            return x.get(key)
        return None

    if "lat" not in sh.columns:
        sh["lat"] = sh["raw"].apply(lambda x: get_raw(x, "lat"))
    if "lng" not in sh.columns:
        sh["lng"] = sh["raw"].apply(lambda x: get_raw(x, "lng"))

    # 오픈/지정일
    if "openDate" in sh.columns:
        sh["openDate"] = pd.to_datetime(sh["openDate"], errors="coerce")
    else:
        sh["openDate"] = pd.to_datetime(
            sh["raw"].apply(lambda x: get_raw(x, "dsignationDate")),
            errors="coerce",
        )

    print("보호소 정제 완료:", sh.shape)
    return sh


# ---------- 유기동물 + 보호소 Join ----------

def merge_abandonments_with_shelters(ab: pd.DataFrame, sh: pd.DataFrame) -> pd.DataFrame:
    print("유기동물 + 보호소 Join(careNm 기준) 중...")
    merged = ab.merge(
        sh[["careNm", "lat", "lng", "careAddr", "divisionNm", "sido", "sigungu"]],
        on="careNm",
        how="left",
        suffixes=("", "_shelter"),
    )
    print("Join 완료:", merged.shape)
    return merged


# ---------- 메인 파이프라인 ----------

def main():
    db = get_mongo_db()
    ab, reg, sh = load_data(db)

    ab_clean = clean_abandonments(ab)
    reg_clean = clean_registrations(reg)
    sh_clean = clean_shelters(sh)

    ab_merged = merge_abandonments_with_shelters(ab_clean, sh_clean)

    # 파일로 저장
    os.makedirs("data", exist_ok=True)
    print("CSV로 저장 중...")
    ab_merged.to_csv("data/clean_abandonments.csv", index=False)
    reg_clean.to_csv("data/clean_registrations.csv", index=False)
    sh_clean.to_csv("data/clean_shelters.csv", index=False)

    print("저장 완료!")
    print(" - clean_abandonments.csv")
    print(" - clean_registrations.csv")
    print(" - clean_shelters.csv")


if __name__ == "__main__":
    main()
