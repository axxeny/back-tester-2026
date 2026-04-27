# CMF Advanced Backtesting Engine for Options

## Directory structure

```
.
├── 3rdparty                    # place holder for 3rd party libraries (downloaded during the build)
├── build                       # local build tree used by CMake
├    ├── bin                    # generated binaries
├    ├── lib                    # generated libs (including those, which are built from 3rd party sources)
├    ├── cfg                    # generated config files (if any)
├    └── include                # generated include files (installed during the build for 3rd party sources)
├── cmake                       # cmake helper scripts
├── config                      # example config files
├── scripts                     # shell (and other) maintenance scripts
├── src                         # source files
├    ├── common                 # common utility files
├    ├── ...                    # ...
├    └── main                   # main() for back-tester app
├── test                        # unit-tests and other tests
├── CMakeLists.txt              # main build script
└── README.md                   # this README
```

## OS

Our primary platform is Linux, but nothing prevents it to be built and run on other OS.
The following commands are for Linux users.
Other users are encouraged to add the corresponding instructions for required steps in this README.

## Build

Install dependencies once:

```
sudo apt install -y cmake g++ clang-format
```

Build using cmake:

```
cmake -B build -S .
cmake --build build -j
```

Local reproducible Feather env:

```bash
uv sync --group feather --no-dev
cmake -S . -B build
cmake --build build -j
```

If repo-local `.venv` has `pyarrow`, CMake auto-detects Arrow headers/libs from it and enables Feather ingest. Without that group, build still works but Feather input stays disabled at runtime.

or

```
mkdir -p build
pushd build
cmake ..
make -j VERBOSE=1
popd
```

## Test

To run unit tests:

```
ctest --test-dir build -j
```

or

```
pushd build
ctest -j
popd
```

or

```
build/bin/test/back-tester-tests
```

## Run

Back-tester:

```
build/bin/back-tester
```

Smoke test against one real zipped Databento file:

```bash
python3 scripts/smoke_ingest.py --zip /path/to/day.zip
```

Hard-variant run against an extracted folder of daily JSON files:

```bash
build/bin/back-tester /path/to/folder --strategy both --preview-limit 10
```

Homework 2 style run with LOB snapshots and bounded final-book output:

```bash
build/bin/back-tester /path/to/folder \
  --strategy both \
  --preview-limit 0 \
  --snapshot-every 500000 \
  --snapshot-depth 5 \
  --max-snapshots 3 \
  --final-books-limit 20
```

Convert raw Databento NDJSON files into Feather:

```bash
.venv/bin/python scripts/convert_to_feather.py \
  /path/to/xeur-eobi-20260310.mbo.json \
  --out-dir build/feather
```

Replay same file from Feather through C++ Arrow ingest:

```bash
build/bin/back-tester build/feather/xeur-eobi-20260310.mbo.feather \
  --preview-limit 0 \
  --snapshot-every 500000 \
  --max-snapshots 2
```

Benchmark all `.mbo.json` members inside one or more input zips and write reports:

```bash
.venv/bin/python scripts/bench_zip_ingest.py \
  --zip /path/to/day-1.zip \
  --zip /path/to/day-2.zip \
  --input-format both \
  --lob-workers 2 \
  --scratch-dir .axxeny-code/tmp \
  --csv-out build/bench/ingest.csv \
  --md-out build/bench/ingest.md
```

Benchmark hard-variant folder ingest for both merge strategies:

```bash
.venv/bin/python scripts/bench_folder_ingest.py \
  --input-dir /path/to/json-folder \
  --input-format both \
  --lob-workers 2 \
  --snapshot-mode async \
  --scratch-dir .axxeny-code/tmp \
  --csv-out build/bench/hard_ingest.csv \
  --md-out build/bench/hard_ingest.md
```

Useful bounded runs:

```bash
.venv/bin/python scripts/bench_zip_ingest.py --task-inbox /path/to/zips --limit-members 1 --scratch-dir .axxeny-code/tmp
.venv/bin/python scripts/bench_zip_ingest.py --task-inbox /path/to/zips --member-substr 20260406 --input-format both --scratch-dir .axxeny-code/tmp
.venv/bin/python scripts/bench_folder_ingest.py --task-inbox /path/to/zips --limit-files 4 --input-format both --lob-workers 2 --scratch-dir .axxeny-code/tmp
```

## Contributing

Install UV, create a virtual environment, and install the project dependencies:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

Then activate the virtual environment and set up the git pre-commit hooks:

```bash
source .venv/bin/activate
pre-commit install
```

After that, formatting and linting will run automatically before each commit.
If the source code does not meet the required formatting rules, the hook will
modify the files and stop the commit, and you will need to stage the updated
changes manually.

To run formatting and linting yourself, use one of these commands:

```bash
pre-commit run --files file.py
pre-commit run --all-files
```

The current pre-commit hooks do the following:
- format and lint C++ code with `clang-format`;
- format and lint Python code with `ruff`;
- strip outputs from Jupyter notebooks.
