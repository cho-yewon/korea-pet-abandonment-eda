# analysis_model.py
import sys
import datetime
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

DATA_PATH = "data/clean_abandonments.csv"

# ---------- 콘솔 출력 + 파일 로그 동시 저장 설정 ----------
log_filename = f"analysis_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
log_file = open(log_filename, "w", encoding="utf-8")

class Tee:
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams:
            s.write(data)
    def flush(self):
        for s in self.streams:
            s.flush()

sys.stdout = Tee(sys.stdout, log_file)
# -------------------------------------------------------


def load_data(path: str = DATA_PATH) -> pd.DataFrame:
    print("📥 데이터 로드 중:", path)
    ab = pd.read_csv(path)
    print(" - 유기동물 데이터 shape:", ab.shape)
    return ab


# 4-1. 기본 분포 / 패턴 분석 ---------------------------------------------

def analyze_basic_patterns(ab: pd.DataFrame) -> None:
    print("\n===== [4-1] 처리결과 분포 (processState 비율) =====")
    print(ab["processState"].value_counts(normalize=True).round(4))

    print("\n===== [4-1] 종(species) 분포 =====")
    print(ab["species"].value_counts().head(10))

    print("\n===== [4-1] 성별(sex) 분포 =====")
    print(ab["sex"].value_counts(dropna=False))

    print("\n===== [4-1] 중성화(neuter) 여부 분포 =====")
    print(ab["neuter"].value_counts(dropna=False))

    print("\n===== [4-1] 연령(age) 기초 통계 =====")
    print(ab["age"].describe())


# 4-2. 예측 모델 구축 (RandomForest 기반) --------------------------------

def build_and_evaluate_model(ab: pd.DataFrame) -> None:
    print("\n===== [4-2] 예측 모델 구축 (processState 예측) =====")

    # 10% 샘플링 (57만 → 5.7만)
    print("\n10% 샘플링 적용 중... (모델 학습 전용)")
    ab_sample = ab.sample(frac=0.1, random_state=42)
    print(" - 샘플링 후 데이터 shape:", ab_sample.shape)

    features = [
        "species", "breed", "sex", "neuter",
        "age", "weight", "month", "season",
        "weekday", "sido", "sigungu",
    ]
    target = "processState"

    df_model = ab_sample[features + [target]].dropna(subset=[target]).copy()

    # 수치형 결측치 보정
    df_model["weight"] = df_model["weight"].fillna(df_model["weight"].mean())
    df_model["age"] = df_model["age"].fillna(df_model["age"].median())

    X = df_model[features].copy()
    y = df_model[target].copy()

    categorical_features = [
        "species", "breed", "sex", "neuter",
        "season", "weekday", "sido", "sigungu",
    ]
    numeric_features = ["age", "weight", "month"]

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("num", "passthrough", numeric_features),
        ]
    )

    model = Pipeline(steps=[
        ("preprocess", preprocess),
        ("clf", RandomForestClassifier(
            n_estimators=200,
            random_state=42,
            n_jobs=-1,
        )),
    ])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    print(" - 학습 데이터 크기:", X_train.shape)
    print(" - 테스트 데이터 크기:", X_test.shape)

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    print("\n[모델 정확도] accuracy:", round(acc, 4))
    print("\n[클래스별 성능 지표] classification_report:")
    print(classification_report(y_test, y_pred, zero_division=0))


# 4-3. 시계열 분석 (Time Series) ----------------------------------------

def analyze_time_series(ab: pd.DataFrame) -> None:
    print("\n===== [4-3] 시계열 분석 (Time Series) =====")

    if "year" in ab.columns:
        yearly = ab.groupby("year")["uid"].count()
        print("\n[연도별 유기동물 발생 건수]")
        print(yearly)

        year_state = ab.groupby(["year", "processState"])["uid"].count()
        print("\n[연도 × 처리결과 발생 건수]")
        print(year_state.head(20))
    else:
        print(" - year 컬럼이 없어 연도별 분석은 생략합니다.")

    monthly = ab.groupby("month")["uid"].count()
    print("\n[월별 유기동물 발생 건수]")
    print(monthly)

    month_state = ab.groupby(["month", "processState"])["uid"].count()
    print("\n[월별 × 처리결과 발생 건수]")
    print(month_state.head(20))


# 4-4. 지역 기반 분석 (Spatial Analysis) --------------------------------

def analyze_spatial(ab: pd.DataFrame) -> None:
    print("\n===== [4-4] 지역 기반 분석 (Spatial Analysis) =====")

    print("\n[시도별 유기동물 발생 건수 TOP 10]")
    sido_cnt = ab.groupby("sido")["uid"].count().sort_values(ascending=False)
    print(sido_cnt.head(10))

    print("\n[시군구별 유기동물 발생 건수 TOP 20]")
    sigungu_cnt = ab.groupby("sigungu")["uid"].count().sort_values(ascending=False)
    print(sigungu_cnt.head(20))

    print("\n[시도 × 처리결과 교차표]")
    crosstab_sido_state = pd.crosstab(ab["sido"], ab["processState"])
    print(crosstab_sido_state.head(10))


# 4-5. 다변수 상관관계 분석 ---------------------------------------------

def analyze_correlations(ab: pd.DataFrame) -> None:
    print("\n===== [4-5] 다변수 상관관계 분석 =====")

    num_cols = ["age", "weight", "month"]
    available = [c for c in num_cols if c in ab.columns]

    if available:
        print("\n[수치형 변수 상관계수]")
        print(ab[available].corr().round(3))
    else:
        print(" - 상관계수 계산 가능한 수치형 컬럼이 없습니다.")

    print("\n[성별(sex) × 처리결과(processState) 교차표]")
    print(pd.crosstab(ab["sex"], ab["processState"]))

    print("\n[중성화(neuter) × 처리결과(processState) 교차표]")
    print(pd.crosstab(ab["neuter"], ab["processState"]))

    print("\n[종(species) × 계절(season) 교차표]")
    print(pd.crosstab(ab["species"], ab["season"]))


# 메인 실행 --------------------------------------------------------------

def main():
    ab = load_data()

    analyze_basic_patterns(ab)      # 4-1
    build_and_evaluate_model(ab)    # 4-2 (샘플링 적용)
    analyze_time_series(ab)         # 4-3
    analyze_spatial(ab)             # 4-4
    analyze_correlations(ab)        # 4-5

    # 로그 파일 닫기
    log_file.close()


if __name__ == "__main__":
    main()
