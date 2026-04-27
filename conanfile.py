from conan import ConanFile
from conan.tools.cmake import CMakeDeps, CMakeToolchain, cmake_layout


class BackTesterConan(ConanFile):
    name = "back-tester"
    version = "0.0.0"
    package_type = "application"

    settings = "os", "arch", "compiler", "build_type"
    options = {
        "with_feather": [True, False],
    }
    default_options = {
        "with_feather": True,
        "simdjson/*:shared": False,
        "catch2/*:shared": False,
        "arrow/*:shared": False,
        "arrow/*:parquet": False,
        "arrow/*:compute": False,
        "arrow/*:dataset_modules": False,
        "arrow/*:with_csv": False,
        "arrow/*:with_json": False,
        "arrow/*:with_thrift": False,
        "arrow/*:with_boost": False,
        "arrow/*:with_mimalloc": False,
    }

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("simdjson/3.10.1")
        self.requires("catch2/3.8.1")
        if self.options.with_feather:
            self.requires("arrow/23.0.1")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        toolchain = CMakeToolchain(self)
        toolchain.user_presets_path = False
        toolchain.cache_variables["BACK_TESTER_FEATHER_MODE"] = (
            "AUTO" if self.options.with_feather else "OFF"
        )
        toolchain.generate()
