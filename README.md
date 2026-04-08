# mtg2f

`mtg2f`는 유전자형 데이터를 PLINK 형식으로 변환하고 MTG2를 사용한 분석을 실행하는 도구입니다.

## 주요 기능

- Illumina Final Report, wide SNP-major 테이블 또는 일반 유전자형 파일을 PLINK 형식으로 변환
- 결측 유전자형 코드, 기본 표현형 및 성별 값 설정
- PLINK QC 파이프라인 후 MTG2 분석 실행
- `plink`, `mtg2`, `gcta` 바이너리 검증 및 위치 확인 기능

## 설치

이 프로젝트는 Python 3.14 환경에서 개발되었으며 3.14 버전 이상을 권장합니다.

```sh
# UV로 가상 환경 생성
uv create .venv --python 3.14

# Windows PowerShell에서 가상 환경 활성화
.venv\Scripts\Activate.ps1

# Linux/macOS에서 가상 환경 활성화
source .venv/bin/activate

# 편집 가능 모드로 설치
uv sync --dev

# 독립 실행형 실행 파일 빌드 (선택 사항)
pyinstaller ./mtg2f.spec
./dist/mtg2f
```

### 의존성

컴파일에 필요한 외부 도구:
* [uv](https://docs.astral.sh/uv/)

런타임에 필요한 외부 도구:

* [PLINK](https://www.cog-genomics.org/plink/1.9/)
* [MTG2](https://sites.google.com/view/s-hong-lee-homepage/mtg2)
* [GCTA](https://yanglab.westlake.edu.cn/software/gcta/)

모든 실행 파일이 시스템 `PATH`에 있는지 확인하거나 환경을 조정하여 사용자 정의 위치를 지정하세요.


## 사용법

PyInstaller로 선 패키징 되었거나 설치 후 `mtg2f`를 명령줄에서 사용할 수 있습니다. 설치하지 않은 경우 venv를 활성화 한 셸에서 `uv run main.py`를 통해 실행할 수 있습니다. 이 경우 [uv](https://docs.astral.sh/uv/) 패키지 매니저가 필요합니다.

```sh
mtg2f check [옵션]
mtg2f convert [옵션] <입력파일> <출력접두사>
mtg2f run [옵션] <입력접두사> <출력접두사>
```

### `check` - 의존성 확인

필수 외부 바이너리(`plink`, `mtg2`, `gcta`)를 찾을 수 있는지 확인하고 경로를 출력합니다. 변환 또는 QC를 실행하기 전에 이 명령어를 사용하세요.

```sh
mtg2f check
```

**옵션:**
- `--verbose, -V`: 상세한 (DEBUG) 로깅 활성화
- `--log <경로>`: 콘솔 출력 외에 로그를 파일로 저장

### `convert` - 유전자형 데이터 변환

유전자형 데이터를 PLINK 형식으로 변환합니다. 참조축군과 실험축군 상관없이 사용할 수 있습니다.

```sh
# Illumina final report를 plink 형식으로 변환
mtg2f convert sample_final_report.txt study

# wide 형식 지정 및 결측값/성별 파라미터 조정
mtg2f convert --format wide --missing NN --sex 3 raw_wide.txt study2

# 최소 유전자형 개수 필터링 적용
mtg2f convert --min-count 5 --phenotype -9 input.txt output
```

**필수 인자:**
- `input`: 입력 유전자형 파일 경로
- `output`: 출력 접두사 (`<접두사>.map`, `<접두사>.ped`, `<접두사>_id.txt` 생성)

**옵션:**
- `-f, --format {illumina,wide,genotype}`: 입력 형식 (기본값: `illumina`)
  - `illumina`: Illumina Final Report 형식
  - `wide`: SNP-major 탭 구분 테이블
  - `genotype`: geno.txt 파일
- `--missing <문자열>`: 입력 파일의 결측 유전자형 문자열 (기본값: `-`, wide 형식은 `NN` 권장)
- `--min-count <정수>`: 최소 유전자형 개수. 이 값 이하로 나타나는 유전자형은 결측으로 설정 (기본값: 0 = 비활성화)
- `--sex {1,2,3}`: 기본 성별 코드 (1=남성, 2=여성, 3=알 수 없음, 기본값: 3)
- `--phenotype <정수>`: 기본 표현형 값 (기본값: -9 = 결측)
- `--verbose, -V`: 상세한 (DEBUG) 로깅 활성화
- `--log <경로>`: 로그를 파일로 저장

**출력 파일:**
- `[output].map`: PLINK MAP 파일
- `[output].ped`: PLINK PED 파일
- `[output]_id.txt`: ID 매핑 파일

### `run` - QC 파이프라인 및 분석 실행

`convert`를 통해 변환된 파일에 대해 파이프라인과 분석을 실행합니다. 실험축군과 참조축군 둘 다 필요합니다. PLINK로 생성한 참조축군 바이너리 데이터가 없는 경우 변환하고 QC를 실행합니다. 이후 GCTA와 MTG2를 사용하여 분석을 진행하고 최종 결과를 csv 파일로 출력합니다.

PLINK, MTG2와 GCTA 프로그램이 필요하며 MTG2 지원 문제로 리눅스에서만 지원됩니다.

```sh
# 기본 실행
mtg2f run study output --ref reference --pheno pheno.fam --cc class.cov --qc qc.cov

# 상세 로그와 함께 실행
mtg2f run study output --ref reference --pheno pheno.fam --cc class.cov --qc qc.cov --verbose
```

**필수 인자:**
- `input`: 변환된 PLINK 파일의 입력 접두사 (예: `study`)
- `output`: QC 결과의 출력 접두사 (기본값: 지정하지 않으면 `<입력>_final`)

**필수 옵션:**
- `--ref <접두사>`: 참조 무리(herd) 데이터 접두사(prefix)
- `--pheno <경로>`: 참조 무리 표현형 FAM 파일 경로
- `--cc <경로>`: 분류 공변량 파일 경로
- `--qc <경로>`: QC 공변량 파일 경로

**선택 옵션:**
- `--verbose, -V`: 상세한 (DEBUG) 로깅 활성화
- `--log <경로>`: 로그를 파일로 저장

**실행 단계:**
0. 데이터 포맷 변환
1. PLINK QC 파이프라인 실행
2. 가지치기된 데이터 비교
3. 유전자형 파일 필터링
4. 무리 데이터 병합
5. GCTA를 사용한 GRM 생성
6. 표현형 데이터 병합
7. MTG2 REML 분석 실행 (다변량)
8. BLUP 및 GEBV 예측 생성

## 공통 옵션

모든 명령어에서 사용 가능한 옵션:

- `--verbose, -V`: DEBUG 레벨 로깅 활성화
- `--log <경로>`: 콘솔 출력 외에 지정된 경로에 로그 저장

## 개발

이 저장소는 `converter/` 디렉토리에 모듈을, `runners/` 디렉토리에 러너를 배치하는 구조로 되어 있습니다. `main.py` 스크립트는 인자 파싱 및 조정을 담당합니다. 패키징에는 `pyproject.toml`을 사용하고 PyInstaller로 독립 실행형 실행 파일을 빌드하려면 `mtg2f.spec`을 사용하세요.

필요에 따라 테스트 및 추가 문서를 추가할 수 있습니다.

## 라이선스

이 프로젝트는 GNU GPL v3 라이선스에 따라 배포됩니다. 자세한 내용은 [LICENSE](LICENSE)를 참조하세요.

