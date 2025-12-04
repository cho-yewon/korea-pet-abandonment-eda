# src/visualization.py
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium

DATA_DIR = "data"
AB_PATH = os.path.join(DATA_DIR, "clean_abandonments.csv")
REG_PATH = os.path.join(DATA_DIR, "clean_registrations.csv")
SH_PATH = os.path.join(DATA_DIR, "clean_shelters.csv")
FIG_DIR = "figures"


def setup_env():
    os.makedirs(FIG_DIR, exist_ok=True)

    sns.set(style="whitegrid")
    plt.rcParams["axes.unicode_minus"] = False

    try:
        plt.rc("font", family="Malgun Gothic")
    except Exception:
        pass


def load_data():
    print(f"📥 유기동물 데이터 로드: {AB_PATH}")
    ab = pd.read_csv(AB_PATH)
    print(" - abandonments shape:", ab.shape)

    print(f"📥 등록현황 데이터 로드: {REG_PATH}")
    reg = pd.read_csv(REG_PATH)
    print(" - registrations shape:", reg.shape)

    print(f"📥 보호소 데이터 로드: {SH_PATH}")
    sh = pd.read_csv(SH_PATH)
    print(" - shelters shape:", sh.shape)

    return ab, reg, sh


# ---------- 5-1. 유기동물 시계열 (연도 / 월 / 계절) ----------

def plot_time_series_ab(ab: pd.DataFrame):
    print("\n📊 [유기동물 시계열] 연도·월·계절별 그래프 생성 중...")

    # 연도별
    if "year" in ab.columns:
        yearly = ab.groupby("year")["uid"].count()
        plt.figure(figsize=(8, 4))
        yearly.plot(marker="o")
        plt.title("연도별 유기동물 발생 추이")
        plt.xlabel("연도")
        plt.ylabel("건수")
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "ab_timeseries_yearly.png"), dpi=200)
        plt.close()

    # 월별
    monthly = ab.groupby("month")["uid"].count().reindex(range(1, 13))
    plt.figure(figsize=(8, 4))
    monthly.plot(kind="line", marker="o")
    plt.title("월별 유기동물 발생 추이")
    plt.xlabel("월")
    plt.ylabel("건수")
    plt.xticks(range(1, 13))
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "ab_timeseries_monthly.png"), dpi=200)
    plt.close()

    # 계절별
    if "season" in ab.columns:
        season_order = ["Spring", "Summer", "Fall", "Winter"]
        season = ab.groupby("season")["uid"].count().reindex(season_order)
        plt.figure(figsize=(6, 4))
        season.plot(kind="bar")
        plt.title("계절별 유기동물 발생 건수")
        plt.xlabel("계절")
        plt.ylabel("건수")
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "ab_timeseries_season.png"), dpi=200)
        plt.close()


# ---------- 5-2. 시도 × 월 Heatmap (유기동물) ----------

def plot_heatmap_sido_month_ab(ab: pd.DataFrame):
    print("\n📊 [Heatmap] 시도 × 월 유기동물 패턴 시각화 중...")

    heat = ab.groupby(["sido", "month"])["uid"].count().unstack(fill_value=0)

    plt.figure(figsize=(12, 8))
    sns.heatmap(
        heat,
        cmap="Blues",
        linewidths=0.3,
        linecolor="lightgrey",
        cbar_kws={"label": "유기동물 건수"},
    )
    plt.title("시도 × 월별 유기동물 발생 Heatmap")
    plt.xlabel("월")
    plt.ylabel("시도")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "ab_heatmap_sido_month.png"), dpi=200)
    plt.close()


# ---------- 5-3. 등록현황 시각화 (시계열 / 지역비교) ----------

def plot_registrations(reg: pd.DataFrame):
    print("\n📊 [등록현황] 출생연도·지역별 등록 패턴 시각화 중...")

    # 출생연도 기준 등록두수 (birthYear가 있다고 가정)
    if "birthYear" in reg.columns:
        # 숫자형으로 변환 (혹시 문자열이면)
        reg_clean = reg.copy()
        reg_clean["birthYear"] = pd.to_numeric(reg_clean["birthYear"], errors="coerce")

        # 말이 되는 연도 범위만 필터링 (예: 1900~2030)
        reg_clean = reg_clean[reg_clean["birthYear"].between(1900, 2030)]

        yearly = (
            reg_clean.groupby("birthYear")["count"]
            .sum()
            .sort_index()
        )

        plt.figure(figsize=(8, 4))
        yearly.plot(marker="o")
        plt.title("출생연도별 등록두수 추이 (1900~2030)")
        plt.xlabel("출생연도")
        plt.ylabel("등록 마릿수")
        plt.xlim(1990, 2030)
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "reg_by_birthyear.png"), dpi=200)
        plt.close()

    # 시도별 등록두수 TOP 10 (기존 그대로)
    reg_sido = reg.groupby("sido")["count"].sum().sort_values(ascending=False)
    plt.figure(figsize=(8, 4))
    reg_sido.head(10).plot(kind="bar")
    plt.title("시도별 등록 마릿수 TOP 10")
    plt.xlabel("시도")
    plt.ylabel("등록 마릿수")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "reg_sido_top10.png"), dpi=200)
    plt.close()


# ---------- 5-4. “등록 vs 유기” 비교 (시도 단위) ----------

def plot_abandon_vs_register_by_sido(ab: pd.DataFrame, reg: pd.DataFrame):
    print("\n📊 [등록 vs 유기] 시도 단위 비교 그래프 생성 중...")

    ab_sido = ab.groupby("sido")["uid"].count().rename("abandon_cnt")
    reg_sido = reg.groupby("sido")["count"].sum().rename("reg_cnt")

    merged = pd.concat([ab_sido, reg_sido], axis=1).dropna()

    # 등록 0 회피
    merged = merged[merged["reg_cnt"] > 0]

    # 비율 (유기 / 등록)
    merged["abandon_per_1000"] = merged["abandon_cnt"] / merged["reg_cnt"] * 1000

    # TOP 10 기준으로 시각화
    top = merged.sort_values("abandon_per_1000", ascending=False).head(10)

    # ① 등록 vs 유기 건수 (이중 바차트)
    plt.figure(figsize=(10, 5))
    idx = range(len(top))
    width = 0.35

    plt.bar([i - width/2 for i in idx], top["reg_cnt"], width=width, label="등록두수")
    plt.bar([i + width/2 for i in idx], top["abandon_cnt"], width=width, label="유기건수")

    plt.xticks(idx, top.index, rotation=45, ha="right")
    plt.title("시도별 등록두수 vs 유기건수 (TOP 10)")
    plt.ylabel("건수")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "sido_reg_vs_abandon.png"), dpi=200)
    plt.close()

    # ② 1,000마리당 유기 비율
    plt.figure(figsize=(10, 5))
    top["abandon_per_1000"].plot(kind="bar")
    plt.title("시도별 1,000마리당 유기 발생 비율 (등록 대비)")
    plt.xlabel("시도")
    plt.ylabel("유기 건수 / 1,000마리")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "sido_abandon_per_1000.png"), dpi=200)
    plt.close()


# ---------- 5-5. 보호소 기반 핫스팟 / 분포 ----------

def plot_shelter_distribution(sh: pd.DataFrame):
    print("\n🗺 [보호소 분포] 보호소 위치·지역별 개수 시각화 중...")

    # 시도/지자체별 보호소 개수 (막대그래프)
    if "orgNm" in sh.columns:
        sh_sido = sh.groupby("orgNm")["uid"].count().sort_values(ascending=False)
        plt.figure(figsize=(8, 4))
        sh_sido.head(15).plot(kind="bar")
        plt.title("지자체(orgNm)별 보호소 수 TOP 15")
        plt.xlabel("지자체")
        plt.ylabel("보호소 수")
        plt.xticks(rotation=60, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "sh_orgNm_top15.png"), dpi=200)
        plt.close()

    # ① Matplotlib 위경도 스캐터 (기존)
    if {"lat", "lng"}.issubset(sh.columns):
        geo = sh.dropna(subset=["lat", "lng"])
        plt.figure(figsize=(8, 8))
        plt.scatter(geo["lng"], geo["lat"], s=20, alpha=0.6)
        plt.title("전국 보호소 위치 분포 (위경도 스캐터)")
        plt.xlabel("경도(lng)")
        plt.ylabel("위도(lat)")
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "sh_map_shelters.png"), dpi=200)
        plt.close()

        # ② folium 지도 위에 찍기 (인터랙티브 지도)
        print("   - folium 기반 HTML 지도 생성 중...")

        # 대한민국 중심 대략 좌표
        center_lat = geo["lat"].mean()
        center_lng = geo["lng"].mean()

        m = folium.Map(
            location=[center_lat, center_lng],
            zoom_start=7,
            tiles="CartoDB positron",  # 깔끔한 배경지도
        )

        for _, row in geo.iterrows():
            folium.CircleMarker(
                location=[row["lat"], row["lng"]],
                radius=3,
                fill=True,
                fill_opacity=0.7,
                popup=row.get("careNm", ""),
            ).add_to(m)

        html_path = os.path.join(FIG_DIR, "sh_map_shelters.html")
        m.save(html_path)
        print(f"   - 보호소 지도 HTML 저장 완료: {html_path}")
    else:
        print(" - lat/lng 컬럼이 없어 지도 기반 시각화를 건너뜁니다.")


# ---------- 5-6. 처리결과 시각화 (유기 데이터 기반) ----------

def plot_process_state(ab: pd.DataFrame):
    print("\n📊 [처리결과] 상태별 비율 / 연도별 비율 추이 시각화 중...")

    state_ratio = ab["processState"].value_counts(normalize=True)
    plt.figure(figsize=(6, 6))
    state_ratio.plot(kind="pie", autopct="%.1f%%")
    plt.ylabel("")
    plt.title("처리결과(processState) 비율")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "ab_process_state_ratio.png"), dpi=200)
    plt.close()

    if "year" in ab.columns:
        year_state = (
            ab.groupby(["year", "processState"])["uid"]
            .count()
            .unstack(fill_value=0)
        )
        year_state_ratio = year_state.div(year_state.sum(axis=1), axis=0)

        plt.figure(figsize=(10, 5))
        plt.stackplot(
            year_state_ratio.index,
            year_state_ratio.T.values,
            labels=year_state_ratio.columns,
        )
        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.title("연도별 처리결과 비율 추이 (stacked area)")
        plt.xlabel("연도")
        plt.ylabel("비율")
        plt.tight_layout()
        plt.savefig(
            os.path.join(FIG_DIR, "ab_year_process_state_ratio.png"), dpi=200
        )
        plt.close()


# ---------- 5-7. Tableau용 집계 데이터 export ----------

def export_for_tableau(ab: pd.DataFrame, reg: pd.DataFrame, sh: pd.DataFrame):
    print("\n📤 [Tableau] 시각화용 집계 CSV export 중...")

    # (1) 유기: 시도 × 월 × 처리결과
    sido_month_state = (
        ab.groupby(["sido", "month", "processState"])["uid"]
        .count()
        .reset_index(name="count")
    )
    sido_month_state.to_csv(
        os.path.join(DATA_DIR, "tableau_ab_sido_month_state.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    # (2) 등록: 시도 × 출생연도
    if "birthYear" in reg.columns:
        reg_sido_year = (
            reg.groupby(["sido", "birthYear"])["count"]
            .sum()
            .reset_index(name="reg_count")
        )
        reg_sido_year.to_csv(
            os.path.join(DATA_DIR, "tableau_reg_sido_birthYear.csv"),
            index=False,
            encoding="utf-8-sig",
        )

    # (3) 보호소 위치별 정보
    if {"lat", "lng", "careNm"}.issubset(sh.columns):
        sh_geo = sh[["careNm", "orgNm", "lat", "lng"]].dropna()
        sh_geo.to_csv(
            os.path.join(DATA_DIR, "tableau_shelters_geo.csv"),
            index=False,
            encoding="utf-8-sig",
        )


# ---------- 5-8. 품종(품종/종) 분포 ----------
def plot_species_distribution(ab: pd.DataFrame):
    print("\n📊 [품종 구분] 개 / 고양이 / 기타 분포 시각화 중...")

    species_clean = (
        ab["species"]
        .astype(str)
        .str.strip()
        .replace("", "기타")   # 혹시 빈 문자열 있을 경우
    )

    species_count = species_clean.value_counts()

    plt.figure(figsize=(8, 4))
    species_count.plot(kind="bar")
    plt.title("개 / 고양이 / 기타 분포")
    plt.xlabel("종")
    plt.ylabel("건수")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "ab_species_dog_cat_other.png"), dpi=200)
    plt.close()


# ---------- main ----------

def main():
    setup_env()
    ab, reg, sh = load_data()

    # 유기동물 기반 시계열 / Heatmap / 처리결과
    plot_time_series_ab(ab)
    plot_heatmap_sido_month_ab(ab)
    plot_process_state(ab)

    # 품종 분포
    plot_species_distribution(ab)

    # 등록현황 기반 시각화 + 유기 vs 등록 비교
    plot_registrations(reg)
    plot_abandon_vs_register_by_sido(ab, reg)

    # 보호소 분포 / 위치 시각화
    plot_shelter_distribution(sh)

    # Tableau용 집계 데이터 같이 export
    export_for_tableau(ab, reg, sh)

    print("\n✅ 5단계 시각화 및 Tableau용 집계 데이터 생성 완료!")
    print(f" - 그래프 PNG: {FIG_DIR} 폴더 확인")
    print(f" - Tableau용 CSV: {DATA_DIR}/tableau_*.csv")


if __name__ == "__main__":
    main()
