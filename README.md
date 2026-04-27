# CMF Advanced Backtesting Engine for Options

## Directory structure

```
.
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

Install build dependencies once:

```
sudo apt install -y cmake g++ clang-format python3-pip
python3 -m pip install --user "conan>=2,<3"
```

Detect a Conan profile once:

```
conan profile detect --force
```

On macOS CommandLineTools-only setups, Conan source builds may also need the Apple
SDK flags exported during `conan install`:

```bash
export CFLAGS='-isysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk'
export CXXFLAGS='-isysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk -isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1'
```

Install C++ dependencies with Conan, then configure and build with the generated toolchain:

```bash
conan install . --build=missing -s build_type=Release
cmake -S . -B build/Release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release -j
```

If you do not need Feather ingest support, skip Arrow entirely:

```bash
conan install . --build=missing -s build_type=Release -o '&:with_feather=False'
cmake -S . -B build/Release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release -j
```

`simdjson`, `Catch2`, and Arrow C++ now come from Conan instead of `ExternalProject` downloads or borrowing headers/libs from a Python wheel.

## Test

To run unit tests:

```
ctest --test-dir build/Release --output-on-failure
```

## Run

Back-tester:

```
build/Release/bin/back-tester
```

Smoke test against one real zipped Databento file:

```bash
python3 scripts/smoke_ingest.py --zip /path/to/day.zip
```

Hard-variant run against an extracted folder of daily JSON files:

```bash
build/Release/bin/back-tester /path/to/folder --strategy both --preview-limit 10
```

Homework 2 style run with LOB snapshots and bounded final-book output:

```bash
build/Release/bin/back-tester /path/to/folder \
  --strategy both \
  --preview-limit 0 \
  --snapshot-every 500000 \
  --snapshot-depth 5 \
  --max-snapshots 3 \
  --final-books-limit 20
```

Convert raw Databento NDJSON files into Feather:

```bash
uv sync --group feather --no-dev
.venv/bin/python scripts/convert_to_feather.py \
  /path/to/xeur-eobi-20260310.mbo.json \
  --out-dir build/feather
```

Replay same file from Feather through C++ Arrow ingest built via Conan:

```bash
build/Release/bin/back-tester build/feather/xeur-eobi-20260310.mbo.feather \
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

Install UV for Python helpers and Conan for C++ dependencies:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
python3 -m pip install --user "conan>=2,<3"
```

Then activate the virtual environment, install the C++ packages for your chosen build type, and set up the git pre-commit hooks:

```bash
source .venv/bin/activate
conan profile detect --force
conan install . --build=missing -s build_type=Release
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
