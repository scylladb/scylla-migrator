#!/bin/bash
# =============================================================================
# ScyllaDB Migrator with MariaDB Support - Build Script
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check for required tools
check_requirements() {
    log_info "Checking build requirements..."
    
    local missing=()
    
    command -v cmake >/dev/null 2>&1 || missing+=("cmake")
    command -v make >/dev/null 2>&1 || missing+=("make")
    command -v sbt >/dev/null 2>&1 || missing+=("sbt")
    command -v java >/dev/null 2>&1 || missing+=("java")
    
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        log_error "Please install them and try again."
        exit 1
    fi
    
    # Check Java version
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 8 ]; then
        log_error "Java 8+ required, found version $java_version"
        exit 1
    fi
    
    log_info "All requirements satisfied"
}

# Build native library
build_native() {
    log_info "Building native C++ library..."
    
    local native_dir="$SCRIPT_DIR/native"
    local build_dir="$native_dir/build"
    
    # Create build directory
    mkdir -p "$build_dir"
    cd "$build_dir"
    
    # Configure with CMake
    log_info "Configuring with CMake..."
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        ..
    
    # Build
    log_info "Compiling native library..."
    make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    
    # Check for output
    if [ -f "libmariadb-scylla-migrator-jni.so" ] || \
       [ -f "libmariadb-scylla-migrator-jni.jnilib" ] || \
       [ -f "mariadb-scylla-migrator-jni.dll" ]; then
        log_info "Native library built successfully"
    else
        log_error "Native library build failed - output not found"
        exit 1
    fi
    
    cd "$SCRIPT_DIR"
}

# Build Scala assembly JAR
build_scala() {
    log_info "Building Scala assembly JAR..."
    
    cd "$SCRIPT_DIR"
    
    # Run sbt assembly (this will also trigger native build)
    sbt clean assembly
    
    # Check for output
    local jar_file=$(find target -name "scylla-migrator-assembly*.jar" 2>/dev/null | head -1)
    if [ -n "$jar_file" ] && [ -f "$jar_file" ]; then
        log_info "Assembly JAR built: $jar_file"
    else
        log_error "Assembly JAR build failed - output not found"
        exit 1
    fi
}

# Run tests
run_tests() {
    log_info "Running tests..."
    
    cd "$SCRIPT_DIR"
    sbt test
    
    log_info "Tests completed"
}

# Clean build artifacts
clean() {
    log_info "Cleaning build artifacts..."
    
    rm -rf "$SCRIPT_DIR/native/build"
    rm -rf "$SCRIPT_DIR/target"
    rm -rf "$SCRIPT_DIR/project/target"
    
    log_info "Clean completed"
}

# Print usage
usage() {
    cat <<EOF
Usage: $0 [command]

Commands:
    all         Build everything (native + scala)
    native      Build native C++ library only
    scala       Build Scala assembly JAR only
    test        Run tests
    clean       Clean build artifacts
    help        Show this help message

Environment variables:
    MARIADB_CONNECTOR_DIR   Path to MariaDB Connector/C installation
    SCYLLA_DRIVER_DIR       Path to ScyllaDB cpp-rs-driver installation
    JAVA_HOME               Path to JDK installation

Examples:
    $0 all                  # Build everything
    $0 native               # Build native library only
    $0 clean && $0 all      # Clean rebuild
EOF
}

# Main
main() {
    local command="${1:-all}"
    
    case "$command" in
        all)
            check_requirements
            build_native
            build_scala
            log_info "Build completed successfully!"
            log_info ""
            log_info "Native library: native/build/libmariadb-scylla-migrator-jni.*"
            log_info "Assembly JAR: target/scala-*/scylla-migrator-assembly*.jar"
            ;;
        native)
            check_requirements
            build_native
            ;;
        scala)
            check_requirements
            build_scala
            ;;
        test)
            check_requirements
            run_tests
            ;;
        clean)
            clean
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
#!/bin/bash
# =============================================================================
# ScyllaDB Migrator with MariaDB Support - Build Script
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check for required tools
check_requirements() {
    log_info "Checking build requirements..."
    
    local missing=()
    
    command -v cmake >/dev/null 2>&1 || missing+=("cmake")
    command -v make >/dev/null 2>&1 || missing+=("make")
    command -v sbt >/dev/null 2>&1 || missing+=("sbt")
    command -v java >/dev/null 2>&1 || missing+=("java")
    
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        log_error "Please install them and try again."
        exit 1
    fi
    
    # Check Java version
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 8 ]; then
        log_error "Java 8+ required, found version $java_version"
        exit 1
    fi
    
    log_info "All requirements satisfied"
}

# Build native library
build_native() {
    log_info "Building native C++ library..."
    
    local native_dir="$SCRIPT_DIR/native"
    local build_dir="$native_dir/build"
    
    # Create build directory
    mkdir -p "$build_dir"
    cd "$build_dir"
    
    # Configure with CMake
    log_info "Configuring with CMake..."
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        ..
    
    # Build
    log_info "Compiling native library..."
    make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    
    # Check for output
    if [ -f "libmariadb-scylla-migrator-jni.so" ] || \
       [ -f "libmariadb-scylla-migrator-jni.jnilib" ] || \
       [ -f "mariadb-scylla-migrator-jni.dll" ]; then
        log_info "Native library built successfully"
    else
        log_error "Native library build failed - output not found"
        exit 1
    fi
    
    cd "$SCRIPT_DIR"
}

# Build Scala assembly JAR
build_scala() {
    log_info "Building Scala assembly JAR..."
    
    cd "$SCRIPT_DIR"
    
    # Run sbt assembly (this will also trigger native build)
    sbt clean assembly
    
    # Check for output
    local jar_file=$(find target -name "scylla-migrator-assembly*.jar" 2>/dev/null | head -1)
    if [ -n "$jar_file" ] && [ -f "$jar_file" ]; then
        log_info "Assembly JAR built: $jar_file"
    else
        log_error "Assembly JAR build failed - output not found"
        exit 1
    fi
}

# Run tests
run_tests() {
    log_info "Running tests..."
    
    cd "$SCRIPT_DIR"
    sbt test
    
    log_info "Tests completed"
}

# Clean build artifacts
clean() {
    log_info "Cleaning build artifacts..."
    
    rm -rf "$SCRIPT_DIR/native/build"
    rm -rf "$SCRIPT_DIR/target"
    rm -rf "$SCRIPT_DIR/project/target"
    
    log_info "Clean completed"
}

# Print usage
usage() {
    cat <<EOF
Usage: $0 [command]

Commands:
    all         Build everything (native + scala)
    native      Build native C++ library only
    scala       Build Scala assembly JAR only
    test        Run tests
    clean       Clean build artifacts
    help        Show this help message

Environment variables:
    MARIADB_CONNECTOR_DIR   Path to MariaDB Connector/C installation
    SCYLLA_DRIVER_DIR       Path to ScyllaDB cpp-rs-driver installation
    JAVA_HOME               Path to JDK installation

Examples:
    $0 all                  # Build everything
    $0 native               # Build native library only
    $0 clean && $0 all      # Clean rebuild
EOF
}

# Main
main() {
    local command="${1:-all}"
    
    case "$command" in
        all)
            check_requirements
            build_native
            build_scala
            log_info "Build completed successfully!"
            log_info ""
            log_info "Native library: native/build/libmariadb-scylla-migrator-jni.*"
            log_info "Assembly JAR: target/scala-*/scylla-migrator-assembly*.jar"
            ;;
        native)
            check_requirements
            build_native
            ;;
        scala)
            check_requirements
            build_scala
            ;;
        test)
            check_requirements
            run_tests
            ;;
        clean)
            clean
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
